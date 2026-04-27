package caddyipinfofree

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/go-co-op/gocron/v2"
	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"
)

const (
	DEFAULT_CRON = "10 16 * * *"

	CRON_NAME_UPDATE         = "update"
	CRON_NAME_INITIAL_UPDATE = "initial-update"

	ID_MODULE_STATE = "caddy.states.ipinfo-free"
)

// Let xcaddy know, there is something to do here
func init() {
	caddy.RegisterModule(IPInfoFreeState{})
	httpcaddyfile.RegisterGlobalOption("ipinfo_free_config", parseCaddyfileConfig)
}

// Define our module with optional json fields that can be stored by caddy
type IPInfoFreeState struct {
	Url              string `json:"url,omitempty"`
	Cron             string `json:"cron,omitempty"`
	Path             string `json:"path,omitempty"`
	ErrorOnInvalidIP bool   `json:"error_on_invalid_ip,omitempty"`

	logger       *zap.Logger       `json:"-"`
	ctx          caddy.Context     `json:"-"`
	scheduler    gocron.Scheduler  `json:"-"`
	db           *maxminddb.Reader `json:"-"`
	etag         string            `json:"-"`
	lastModified string            `json:"-"`
}

// CaddyModule returns the Caddy module information
func (IPInfoFreeState) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  ID_MODULE_STATE,
		New: func() caddy.Module { return new(IPInfoFreeState) },
	}
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler
func (m *IPInfoFreeState) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	// Consume directive as we only have one anyway
	d.Next()
	// Consume next token to determine block or direct url
	var possibleUrl string
	if d.Args(&possibleUrl) {
		// If not block, we don't expect more tokens after url
		if d.NextArg() {
			return d.ArgErr()
		}
		// If last token, remember value as url
		m.Url = possibleUrl
		return nil
	}
	// Iterate of remaining tokens to consume config block
	for d.Next() {
		var value string
		// Get current token value as key
		key := d.Val()
		// Consume left over arguments
		if !d.Args(&value) {
			fmt.Println(key)
			continue
		}
		// Consume all config keys we accept
		switch key {
		case "url":
			m.Url = value
		case "cron":
			m.Cron = value
		case "path":
			m.Path = value
		case "error_on_invalid_ip":
			{
				// Parse value with strconv
				val, err := strconv.ParseBool(value)
				if err != nil {
					return d.WrapErr(err)
				}
				m.ErrorOnInvalidIP = val
			}
		default:
			// If key not known, throw error
			return d.ArgErr()
		}
	}

	return nil
}

func parseCaddyfileConfig(d *caddyfile.Dispenser, _ any) (any, error) {
	// Initialize an empty module
	m := new(IPInfoFreeState)
	// Extract values from caddyfile
	err := m.UnmarshalCaddyfile(d)
	// Return new app from module with possible error
	return httpcaddyfile.App{
		Name:  ID_MODULE_STATE,
		Value: caddyconfig.JSON(m, nil),
	}, err
}

func validateIPInfoFreeUrl(givenUrl string) (*url.URL, error) {
	if givenUrl == "" {
		return nil, errors.New("ipinfo_free_config: url is required")
	}
	u, err := url.Parse(givenUrl)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" || u.Host == "" {
		return u, errors.New("ipinfo_free_config: url must be absolute (scheme + host)")
	}
	return u, nil
}

func (m *IPInfoFreeState) Validate() error {
	parsedUrl, err := validateIPInfoFreeUrl(m.Url)
	if err != nil {
		return err
	}
	m.logger.Info("ipinfo configured to use", zap.String("url", parsedUrl.String()))

	if _, err := cron.ParseStandard(m.Cron); err != nil {
		return err
	}

	return nil
}

func (m *IPInfoFreeState) getFilepath() string {
	u, err := url.Parse(m.Url)
	if err != nil {
		return path.Join(m.Path, "database.mmdb")
	}
	name := path.Base(u.Path)
	if name == "." || name == "/" || name == "" {
		name = "database.mmdb"
	}
	return path.Join(m.Path, name)
}

type databaseMeta struct {
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
}

func (m *IPInfoFreeState) metaFilepath() string {
	return m.getFilepath() + ".meta"
}

func (m *IPInfoFreeState) loadMeta() {
	data, err := os.ReadFile(m.metaFilepath())
	if err != nil {
		return
	}
	var meta databaseMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		m.logger.Warn("could not parse database meta sidecar", zap.Error(err))
		return
	}
	m.etag = meta.ETag
	m.lastModified = meta.LastModified
}

func (m *IPInfoFreeState) saveMeta(etag, lastModified string) {
	data, err := json.Marshal(databaseMeta{ETag: etag, LastModified: lastModified})
	if err != nil {
		m.logger.Warn("could not encode database meta sidecar", zap.Error(err))
		return
	}
	if err := os.WriteFile(m.metaFilepath(), data, 0644); err != nil {
		m.logger.Warn("could not write database meta sidecar", zap.Error(err))
	}
}

func (m *IPInfoFreeState) checkIfUpdateIsNecessary() (necessary bool, newETag, newLastModified string, err error) {
	if _, statErr := os.Stat(m.getFilepath()); statErr != nil {
		return true, "", "", nil
	}

	req, err := http.NewRequestWithContext(m.ctx, http.MethodHead, m.Url, nil)
	if err != nil {
		return false, "", "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		m.logger.Debug("HEAD did not return 200, treating as update-needed",
			zap.Int("status", resp.StatusCode))
		return true, "", "", nil
	}

	newETag = resp.Header.Get("ETag")
	newLastModified = resp.Header.Get("Last-Modified")

	if newETag == "" && newLastModified == "" {
		return true, "", "", nil
	}

	if newETag != "" && newETag == m.etag {
		return false, newETag, newLastModified, nil
	}
	if newLastModified != "" && newLastModified == m.lastModified {
		return false, newETag, newLastModified, nil
	}
	return true, newETag, newLastModified, nil
}

func (m *IPInfoFreeState) runUpdate() error {
	// Lazy-load existing on-disk db on first run
	if _, err := os.Stat(m.getFilepath()); m.db == nil && err == nil {
		if newDb, err := maxminddb.Open(m.getFilepath()); err == nil {
			m.db = newDb
		}
	}

	necessary, newETag, newLastModified, err := m.checkIfUpdateIsNecessary()
	if err != nil {
		return err
	}
	if !necessary {
		return nil
	}

	m.logger.Debug("downloading database", zap.String("url", m.Url))

	req, err := http.NewRequestWithContext(m.ctx, http.MethodGet, m.Url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch database: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code from database url: %d", resp.StatusCode)
	}

	databaseFilepath := m.getFilepath()
	os.Rename(databaseFilepath, databaseFilepath+".old")

	f, err := os.Create(databaseFilepath)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	f.Close()

	newDb, err := maxminddb.Open(databaseFilepath)
	if err != nil {
		return err
	}

	oldDb := m.db
	m.db = newDb
	if oldDb != nil {
		oldDb.Close()
	}

	// GET response validators take precedence if present
	if v := resp.Header.Get("ETag"); v != "" {
		newETag = v
	}
	if v := resp.Header.Get("Last-Modified"); v != "" {
		newLastModified = v
	}
	m.etag = newETag
	m.lastModified = newLastModified
	m.saveMeta(newETag, newLastModified)

	os.Remove(databaseFilepath + ".old")

	m.logger.Info("database updated", zap.String("filepath", databaseFilepath))
	return nil
}

func (m *IPInfoFreeState) Provision(ctx caddy.Context) error {
	// Remember logger and context
	m.logger = ctx.Logger()
	m.ctx = ctx
	// Fallback for contab value
	m.Cron = cmp.Or(m.Cron, DEFAULT_CRON)
	// Path fallback to random temporary path
	m.Path = cmp.Or(m.Path, path.Join(os.TempDir(), "caddy_ipinfo_free"))
	// Initialize scheduler
	if scheduler, err := gocron.NewScheduler(
		gocron.WithLocation(time.UTC),
		gocron.WithLogger(newZapGocronLogger(m.logger.Name(), m.logger)),
	); err != nil {
		return err
	} else {
		m.scheduler = scheduler
	}
	// Initialize update job
	if _, err := m.scheduler.NewJob(
		gocron.CronJob(m.Cron, false),
		gocron.NewTask(
			errorToLogsWrapper(m.logger, m.runUpdate),
		),
		gocron.WithName(CRON_NAME_UPDATE),
	); err != nil {
		return err
	}
	// Initialize initial update run
	if _, err := m.scheduler.NewJob(
		gocron.OneTimeJob(
			gocron.OneTimeJobStartImmediately(),
		),
		gocron.NewTask(
			errorToLogsWrapper(m.logger, m.runUpdate),
		),
		gocron.WithName(CRON_NAME_INITIAL_UPDATE),
	); err != nil {
		return err
	}
	// Make sure target path exists
	if err := os.MkdirAll(m.Path, os.FileMode(0744)); err != nil {
		return err
	}
	// Load any persisted HEAD validators from a previous run
	m.loadMeta()

	return nil
}

func (m *IPInfoFreeState) Start() error {
	// Start scheduler
	m.scheduler.Start()
	return nil
}

func (m *IPInfoFreeState) Stop() error {
	// Stop scheduler and currently running jobs
	m.scheduler.StopJobs()
	return nil
}

func (m *IPInfoFreeState) Cleanup() error {
	// Cleanup the scheduler
	if err := m.scheduler.Shutdown(); err != nil {
		return err
	}
	// Ensure there is a database to cleanup
	if m.db != nil {
		// Close database for cleanup
		if err := m.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Interface guards
var (
	_ caddy.Module          = (*IPInfoFreeState)(nil)
	_ caddy.Provisioner     = (*IPInfoFreeState)(nil)
	_ caddy.CleanerUpper    = (*IPInfoFreeState)(nil)
	_ caddy.Validator       = (*IPInfoFreeState)(nil)
	_ caddyfile.Unmarshaler = (*IPInfoFreeState)(nil)
)
