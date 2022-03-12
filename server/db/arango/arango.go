package arango

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/tinode/chat/server/auth"
	"github.com/tinode/chat/server/db/common"
	"github.com/tinode/chat/server/store"
	t "github.com/tinode/chat/server/store/types"
	"gitlab.com/gfxlabs/goutil/ptr"
	"gitlab.com/gfxlabs/structs"

	driver "github.com/arangodb/go-driver"
	ahttp "github.com/arangodb/go-driver/http"
)

// adapter holds ArangoDB connection data.
type adapter struct {
	conn   driver.Connection
	client driver.Client
	db     driver.Database
	dbName string
	// Maximum number of records to return
	maxResults int
	// Maximum number of message records to return
	maxMessageResults int
	version           int
	ctx               context.Context
	useTransactions   bool
	collections       struct {
		users         driver.Collection
		auth          driver.Collection
		topics        driver.Collection
		subscriptions driver.Collection
		messages      driver.Collection
		dellog        driver.Collection
		credentials   driver.Collection
		fileuploads   driver.Collection
		kvmeta        driver.Collection
	}
}

const (
	defaultHost     = "localhost:8529"
	defaultDatabase = "tinode"

	adpVersion  = 112
	adapterName = "arango"

	defaultMaxResults        = 1024
	defaultMaxMessageResults = 128
)

type configType struct {
	Addresses      interface{} `json:"addresses,omitempty"`
	ConnectTimeout int         `json:"timeout,omitempty"`

	// Options separately from ClientOptions (custom options):
	Database string `json:"database,omitempty"`

	AuthMechanism string `json:"auth_mechanism,omitempty"` // ignored, only support basic auth
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`

	UseTLS             bool   `json:"tls,omitempty"`
	TlsCertFile        string `json:"tls_cert_file,omitempty"`
	TlsCertString      string `json:"tls_cert_string,omitempty"`
	TlsPrivateKey      string `json:"tls_private_key,omitempty"`
	InsecureSkipVerify bool   `json:"tls_skip_verify,omitempty"`
	// If this value is > 0, automatic synchronization is started on a go routine.
	// This feature requires ArangoDB 3.1.15 or up.
	// in seconds
	SynchronizeEndpointsIntervalSeconds int `json:"synchonize_endpoints_interval_seconds"`
}

// Open initializes arangodb session
func (a *adapter) Open(jsonconfig json.RawMessage) error {
	if a.conn != nil {
		return errors.New("adapter arangodb is already connected")
	}

	if len(jsonconfig) < 2 {
		return errors.New("adapter arangodb missing config")
	}

	var err error
	var config configType
	if err = json.Unmarshal(jsonconfig, &config); err != nil {
		return errors.New("adapter arangodb failed to parse config: " + err.Error())
	}

	var connOpts ahttp.ConnectionConfig
	if config.Addresses == nil {
		connOpts.Endpoints = []string{defaultHost}
	} else if host, ok := config.Addresses.(string); ok {
		connOpts.Endpoints = []string{host}
	} else if ihosts, ok := config.Addresses.([]interface{}); ok && len(ihosts) > 0 {
		hosts := make([]string, len(ihosts))
		for i, ih := range ihosts {
			h, ok := ih.(string)
			if !ok || h == "" {
				return errors.New("adapter arangodb invalid config.Addresses value")
			}
			hosts[i] = h
		}
		connOpts.Endpoints = hosts
	} else {
		return errors.New("adapter arangodb failed to parse config.Addresses")
	}
	if config.UseTLS {
		tlsConfig := tls.Config{
			InsecureSkipVerify: config.InsecureSkipVerify,
		}
		if config.TlsCertFile != "" {
			cert, err := tls.LoadX509KeyPair(config.TlsCertFile, config.TlsPrivateKey)
			if err != nil {
				return err
			}
			tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
		}
		if config.TlsCertString != "" {
			caCertificate, err := base64.StdEncoding.DecodeString(config.TlsCertString)
			if err != nil {
				return err
			}
			certpool := x509.NewCertPool()
			if success := certpool.AppendCertsFromPEM(caCertificate); !success {
				return errors.New("invalid cert")
			}
			tlsConfig.RootCAs = certpool
		}
		connOpts.TLSConfig = &tlsConfig
	}

	if config.Database == "" {
		a.dbName = defaultDatabase
	} else {
		a.dbName = config.Database
	}
	a.conn, err = ahttp.NewConnection(connOpts)
	if err != nil {
		return err
	}
	var clientOpts driver.ClientConfig
	clientOpts.SynchronizeEndpointsInterval = time.Second * time.Duration(config.SynchronizeEndpointsIntervalSeconds)
	clientOpts.Connection = a.conn
	if config.Username != "" {
		clientOpts.Authentication = driver.BasicAuthentication(config.Username, config.Password)
	}
	if a.maxResults <= 0 {
		a.maxResults = defaultMaxResults
	}
	if a.maxMessageResults <= 0 {
		a.maxMessageResults = defaultMaxMessageResults
	}
	a.ctx = context.Background()
	a.client, err = driver.NewClient(clientOpts)
	if err != nil {
		return err
	}
	ok, err := a.client.DatabaseExists(a.ctx, a.dbName)
	if err != nil {
		return err
	}
	if !ok {
		_, err := a.client.CreateDatabase(a.ctx, a.dbName, nil)
		if err != nil {
			return err
		}
	}
	a.db, err = a.client.Database(a.ctx, a.dbName)
	if err != nil {
		return err
	}

	collections := []string{
		"kvmeta",
		"users",
		"auth",
		"topics",
		"subscriptions",
		"messages",
		"dellog",
		"credentials",
		"fileuploads",
	}
	for _, v := range collections {
		collectionOptions := &driver.CreateCollectionOptions{}
		coll, err := a.db.Collection(a.ctx, v)
		if err != nil {
			if driver.IsNotFound(err) {
				coll, err = a.db.CreateCollection(a.ctx, v, collectionOptions)
				if err != nil {
					log.Println(err)
				}
			} else {
				log.Println(err)
			}
		}
		switch v {
		case "kvmeta":
			a.collections.kvmeta = coll
		case "users":
			a.collections.users = coll
		case "auth":
			a.collections.auth = coll
		case "topics":
			a.collections.topics = coll
		case "subscriptions":
			a.collections.subscriptions = coll
		case "messages":
			a.collections.messages = coll
		case "dellog":
			a.collections.dellog = coll
		case "credentials":
			a.collections.credentials = coll
		case "fileuploads":
			a.collections.fileuploads = coll
		default:
		}
	}
	return nil
}

// Close the adapter
func (a *adapter) Close() error {
	var err error
	if a.conn != nil {
		a.conn = nil
		a.version = -1
	}
	return err
}

// IsOpen checks if the adapter is ready for use
func (a *adapter) IsOpen() bool {
	return a.conn != nil
}

// GetDbVersion returns current database version.
func (a *adapter) GetDbVersion() (int, error) {
	if a.version > 0 {
		return a.version, nil
	}

	var result struct {
		Key   string `json:"_key"`
		Value int    `json:"value"`
	}
	if _, err := a.collections.kvmeta.ReadDocument(a.ctx, "version", &result); err != nil {
		return -1, err
	}
	a.version = result.Value
	return result.Value, nil
}

// CheckDbVersion checks if the actual database version matches adapter version.
func (a *adapter) CheckDbVersion() error {
	version, err := a.GetDbVersion()
	if err != nil {
		return err
	}

	if version != adpVersion {
		return errors.New("Invalid database version " + strconv.Itoa(version) +
			". Expected " + strconv.Itoa(adpVersion))
	}

	return nil
}

// Version returns adapter version
func (a *adapter) Version() int {
	return adpVersion
}

// DB connection stats object.
func (a *adapter) Stats() interface{} {
	if a.db == nil {
		return nil
	}

	return map[string]interface{}{"serverStatus": 1, "other": "not yet implemented"}
}

// GetName returns the name of the adapter
func (a *adapter) GetName() string {
	return adapterName
}

// SetMaxResults configures how many results can be returned in a single DB call.
func (a *adapter) SetMaxResults(val int) error {
	if val <= 0 {
		a.maxResults = defaultMaxResults
	} else {
		a.maxResults = val
	}

	return nil
}

// CreateDb creates the database optionally dropping an existing database first.
func (a *adapter) CreateDb(reset bool) error {
	if reset {
		//drop the database
		if colls, err := a.db.Collections(a.ctx); err == nil {
			for _, coll := range colls {
				err = coll.Remove(a.ctx)
				if err != nil {
					continue
				}
			}
		} else {
			return err
		}
	} else if a.isDbInitialized() {
		return errors.New("Database already initialized")
	}

	// list of collections to create
	collections := []string{
		"kvmeta",
		"users",
		"auth",
		"topics",
		"subscriptions",
		"messages",
		"dellog",
		"credentials",
		"fileuploads",
	}
	for _, v := range collections {
		collectionOptions := &driver.CreateCollectionOptions{}
		switch v {
		case "messages":
			collectionOptions.CacheEnabled = ptr.Bool(true)
		default:
		}
		coll, err := a.db.Collection(a.ctx, v)
		if err != nil {
			if driver.IsNotFound(err) {
				coll, err = a.db.CreateCollection(a.ctx, v, collectionOptions)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		persistent := []string{}
		switch v {
		case "users":
			a.collections.users = coll
			persistent = append(persistent, "Tags[*]", "DeletedAt")
			_, _, err = coll.EnsurePersistentIndex(a.ctx,
				[]string{"deviceids[*]"},
				&driver.EnsurePersistentIndexOptions{
					InBackground: true,
					Unique:       true,
					Name:         "IDX_" + "deviceids",
				},
			)
			if err != nil {
				return err
			}
		case "auth":
			a.collections.auth = coll
			persistent = append(persistent, "userid")
		case "topics":
			a.collections.topics = coll
			persistent = append(persistent, "Owner", "Tags[*]")
		case "subscriptions":
			a.collections.subscriptions = coll
			persistent = append(persistent, "User", "Topic", "ModeWant", "ModeGiven")
		case "messages":
			a.collections.messages = coll
			persistent = append(persistent, "SeqId", "From", "CreatedAt")
			_, _, err = coll.EnsurePersistentIndex(a.ctx,
				[]string{"Topic", "SeqId"},
				&driver.EnsurePersistentIndexOptions{
					InBackground: true,
					Name:         "IDX_" + "topic_seqid",
				},
			)
			if err != nil {
				return err
			}
			persistent = append(persistent, "Topic", "DelId")
			_, _, err = coll.EnsurePersistentIndex(a.ctx,
				[]string{"topic", "delid"},
				&driver.EnsurePersistentIndexOptions{
					InBackground: true,
					Name:         "IDX_" + "topic_delid",
				},
			)
			if err != nil {
				return err
			}
		case "dellog":
			a.collections.dellog = coll
			persistent = append(persistent, "Topic", "DelId")
			_, _, err = coll.EnsurePersistentIndex(a.ctx,
				[]string{"Topic", "DelId"},
				&driver.EnsurePersistentIndexOptions{
					InBackground: true,
					Name:         "IDX_" + "topic_delid",
				},
			)
			if err != nil {
				return err
			}
		case "credentials":
			a.collections.credentials = coll
			persistent = append(persistent, "User")
		case "fileuploads":
			a.collections.fileuploads = coll
			persistent = append(persistent, "User", "UseCount")
		case "kvmeta":
			a.collections.kvmeta = coll
			_, err := coll.CreateDocument(a.ctx, map[string]interface{}{"_key": "version", "value": adpVersion})
			if err != nil {
				return err
			}
		default:
		}
		for _, f := range persistent {
			tf := strings.ReplaceAll(f, "[", "")
			tf = strings.ReplaceAll(tf, "]", "")
			tf = strings.ReplaceAll(tf, "*", "")
			tf = strings.ReplaceAll(tf, "_array", "")
			_, _, err = coll.EnsurePersistentIndex(a.ctx,
				[]string{f},
				&driver.EnsurePersistentIndexOptions{
					InBackground: true,
					Name:         "IDX_" + tf,
				},
			)
			if err != nil {
				return err
			}
		}
	}
	// Collection "kvmeta" with metadata key-value pairs.
	// Key in "_key" field.
	// Record current DB version.

	// Create system topic 'sys'.
	return createSystemTopic(a)
}

// UpgradeDb upgrades database to the current adapter version.
func (a *adapter) UpgradeDb() error {
	bumpVersion := func(a *adapter, x int) error {
		if err := a.updateDbVersion(x); err != nil {
			return err
		}
		_, err := a.GetDbVersion()
		return err
	}

	_, err := a.GetDbVersion()
	if err != nil {
		return err
	}

	if a.version == 111 {
		// Just bump the version to keep in line with MySQL.
		if err := bumpVersion(a, 112); err != nil {
			return err
		}
	}

	if a.version != adpVersion {
		return errors.New("Failed to perform database upgrade to version " + strconv.Itoa(adpVersion) +
			". DB is still at " + strconv.Itoa(a.version))
	}
	return nil
}

func (a *adapter) updateDbVersion(v int) error {
	a.version = -1
	_, err := a.collections.kvmeta.UpdateDocument(a.ctx,
		"version",
		map[string]interface{}{"value": v},
	)
	return err
}

// Create system topic 'sys'.
func createSystemTopic(a *adapter) error {
	nowt := t.TimeNow()
	topicmap := structs.Map(&t.Topic{
		ObjHeader: t.ObjHeader{
			Id:        "sys",
			CreatedAt: nowt,
			UpdatedAt: nowt},
		TouchedAt: nowt,
		Access:    t.DefaultAccess{Auth: t.ModeNone, Anon: t.ModeNone},
		Public:    map[string]interface{}{"fn": "System"},
	})
	topicmap["_key"] = "sys"
	_, err := a.collections.topics.CreateDocument(a.ctx, topicmap)
	return err
}

// User management

// UserCreate creates user record
func (a *adapter) UserCreate(usr *t.User) error {
	usermap := structs.Map(&usr)
	usermap["_key"] = usr.Id
	if _, err := a.collections.users.CreateDocument(a.ctx, usr); err != nil {
		return err
	}
	return nil
}

// UserGet fetches a single user by user id. If user is not found it returns (nil, nil)
func (a *adapter) UserGet(id t.Uid) (*t.User, error) {
	var user t.User
	if _, err := a.collections.users.ReadDocument(a.ctx, id.String(), &user); err != nil {
		return nil, err
	}
	return &user, nil
}

// UserGetAll returns user records for a given list of user IDs
func (a *adapter) UserGetAll(ids ...t.Uid) ([]t.User, error) {
	uids := make([]string, len(ids))
	for i, id := range ids {
		uids[i] = id.String()
	}
	users := make([]t.User, len(uids))
	_, _, err := a.collections.users.ReadDocuments(a.ctx, uids, users)
	if err != nil {
		return nil, err
	}
	return users, nil
}

// UserDelete deletes user record.
func (a *adapter) UserDelete(uid t.Uid, hard bool) error {
	// Select topics where the user is the owner.
	cursor, err := a.QueryManyf(`for d in topics
	filter d.Owner == "%s"
	return d
	`, uid.String())
	topics := []*t.Topic{}
	for cursor.HasMore() {
		res := &t.Topic{}
		_, err := cursor.ReadDocument(a.ctx, res)
		if err != nil {
			return err
		}
		topics = append(topics, res)
	}
	topicStrings := make([]string, len(topics))
	for i, t2 := range topics {
		topicStrings[i] = t2.Id

	}
	if a.useTransactions {
		tid, err := a.db.BeginTransaction(a.ctx, driver.TransactionCollections{
			Write: []string{"dellog", "messages", "subscriptions", "topics"},
		}, &driver.BeginTransactionOptions{LockTimeout: 10 * time.Second})
		if err != nil {
			return err
		}
		defer a.db.CommitTransaction(a.ctx, tid, nil)
	}
	if hard {
		// 1. Delete dellog
		// TODO: 2. Decrement fileuploads.
		// 3. Delete all messages.
		// 4. Delete subscriptions.

		for _, topic := range topics {
			if err = a.QueryOnef(`for d in dellog
			filter d.Topic == "%s"
			remove d in dellog
			`, nil, topic.Id); err != nil {
				return err
			}
			if err = a.QueryOnef(`for d in subscriptions
			filter d.Topic == "%s"
			remove d in subscriptions
			`, nil, topic.Id); err != nil {
				return err
			}
			if err = a.QueryOnef(`for d in messages
			filter d.Topic == "%s"
			remove d in messages
			`, nil, topic.Id); err != nil {
				return err
			}
			if err = a.QueryOnef(`for d in topics
			filter d._key == "%s"
			remove d in topics
			`, nil, topic.Id); err != nil {
				return err
			}
		}
		// Select all other topics where the user is a subscriber.
		// Delete user's dellog entries.

		// Delete user's markings of soft-deleted messages

		// Delete user's subscriptions in all topics.
		if err = a.QueryOnef(`for d in subscriptions
			filter d.User == "%s"
			remove d in subscriptions
			`, nil, uid.String()); err != nil {
			return err
		}

		// Delete user's authentication records.
		if err = a.QueryOnef(`for d in auth
			filter d.UserId == "%s"
			remove d in auth
			`, nil, uid.String()); err != nil {
			return err
		}

		// Delete credentials.
		if err = a.QueryOnef(`for d in credentials
			filter d.User == "%s"
			remove d in credentials 
			`, nil, uid.String()); err != nil {
			return err
		}

		// TODO: Delete avatar (decrement use counter).

		// And finally delete the user.
		if err = a.QueryOnef(`for d in users
			filter d._key == "%s"
			remove d in users
			`, nil, uid.String()); err != nil {
			return err
		}
	} else {
		if err = a.QueryOnef(`for d in subscriptions
			filter d.User == "%s"
			remove d in subscriptions
			`, nil, uid.String()); err != nil {
			return err
		}

		//TODO: Disable subscriptions for topics where the user is the owner.

		//TODO: Disable topics where the user is the owner.

		//TOTO: Finally disable the user.
	}
	return err
}

// UserUpdate updates user record
func (a *adapter) UserUpdate(uid t.Uid, update map[string]interface{}) error {
	_, err := a.collections.users.UpdateDocument(a.ctx, uid.String(), update)
	if err != nil {
		return err
	}
	return err
}

// UserUpdateTags adds, removes, or resets user's tags
func (a *adapter) UserUpdateTags(uid t.Uid, add, remove, reset []string) ([]string, error) {
	// Compare to nil vs checking for zero length: zero length reset is valid.
	if reset != nil {
		// Replace Tags with the new value
		return reset, a.UserUpdate(uid, map[string]interface{}{"Tags": reset})
	}
	out := []string{}
	for _, v := range add {
		err := a.QueryOnef(""+
			"	for g in users"+
			`	filter g._key == "%s"`+
			`	let base = is_array(g.Tags) ? (unique(append(g.Tags, "%s", true))) : (["%s"])`+
			"	update g with {Tags: base} in users"+
			"	return base", &out, uid.String(), v, v)
		if err != nil {
			return nil, err
		}
	}
	for _, v := range remove {
		err := a.QueryOnef(""+
			"	for g in users"+
			`	filter g._key == "%s"`+
			`	let base = is_array(g.Tags) ? (remove_value(g.Tags, "%s")) : ([])`+
			"	update g with {Tags: base} in users"+
			"	return base", &out, uid.String(), v)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// UserGetByCred returns user ID for the given validated credential.
func (a *adapter) UserGetByCred(method, value string) (t.Uid, error) {
	userId := make(map[string]interface{})
	_, err := a.collections.credentials.ReadDocument(a.ctx,
		method+":"+value,
		&userId)
	if err != nil {
		if driver.IsNotFound(err) {
			return t.ZeroUid, nil
		}
		return t.ZeroUid, err
	}
	val, ok := userId["User"]
	if !ok {
		return t.ZeroUid, t.ErrNotFound
	}
	cast, ok := val.(string)
	if !ok {
		return t.ZeroUid, t.ErrNotFound
	}
	return t.ParseUid(cast), nil
}

// TODO: UserUnreadCount returns the total number of unread messages in all topics with
// the R permission.
func (a *adapter) UserUnreadCount(uid t.Uid) (int, error) {
	querystring := `for d in subscriptions
	for t in topics
	filter d.User == "%s" && t._key == d.Topic
	filter t.State != "%d" && (d.DeletedAt == "" || d.DeletedAt == null)
	filter contains(d.ModeWant, "R") && contains(d.ModeGiven, "R")
	collect aggregate seqId = sum(t.SeqId), readSeqId = sum(d.ReadSeqId) 
	return seqId - readSeqId
	`
	var ans int
	err := a.QueryOnef(querystring, &ans, uid.String(), t.StateDeleted)
	return ans, err
}

// Credential management

// CredUpsert adds or updates a validation record. Returns true if inserted, false if updated.
// 1. if credential is validated:
// 1.1 Hard-delete unconfirmed equivalent record, if exists.
// 1.2 Insert new. Report error if duplicate.
// 2. if credential is not validated:
// 2.1 Check if validated equivalent exist. If so, report an error.
// 2.2 Soft-delete all unvalidated records of the same method.
// 2.3 Undelete existing credential. Return if successful.
// 2.4 Insert new credential record.
func (a *adapter) CredUpsert(cred *t.Credential) (bool, error) {
	credCollection := a.collections.credentials
	cred.Id = cred.Method + ":" + cred.Value

	if !cred.Done {
		// Check if the same credential is already validated.
		var result1 t.Credential
		_, err := credCollection.ReadDocument(a.ctx, cred.Id, &result1)
		if result1 != (t.Credential{}) {
			// Someone has already validated this credential.
			return false, t.ErrDuplicate
		}
		if err != nil {
			if !driver.IsNotFound(err) { // if no result -> continue
				return false, err
			}
		}
		// TODO: Soft-delete all unvalidated records of this user and method.
		// If credential is not confirmed, it should not block others
		// from attempting to validate it: make index user-unique instead of global-unique.
		cred.Id = cred.User + ":" + cred.Id
		// Check if this credential has already been added by the user.
		ok, err := credCollection.DocumentExists(a.ctx, cred.Id)
		if err != nil {
			return false, err
		}
		if ok {
			_, err = credCollection.UpdateDocument(a.ctx,
				cred.Id,
				map[string]interface{}{
					"DeletedAt": "",
					"UpdatedAt": cred.UpdatedAt,
					"Resp":      cred.Resp})
			if err != nil {
				return false, err
			}
			return false, nil
		}
	} else {
		// Hard-delete potentially present unvalidated credential.
		_, err := credCollection.RemoveDocument(a.ctx, cred.User+":"+cred.Id)
		if !driver.IsNotFound(err) { // if no result -> continue
			if err != nil {
				return false, err
			}
		}
	}
	credmap := structs.Map(*cred)
	credmap["DeletedAt"] = ""
	credmap["UpdatedAt"] = cred.UpdatedAt
	credmap["Resp"] = cred.Resp
	credmap["_key"] = cred.Id
	_, err := credCollection.CreateDocument(a.ctx, credmap)
	if driver.IsConflict(err) {
		return true, t.ErrDuplicate
	}
	return true, nil
}

// CredGetActive returns the currently active credential record for the given method.
func (a *adapter) CredGetActive(uid t.Uid, method string) (*t.Credential, error) {
	var cred t.Credential
	if err := a.QueryOnef(`for d in credentials
	filter d.User == "%s" && d.Method == "%s" && d.Done != true
	return d`, &cred, uid.String(), method); err != nil {
		if driver.IsNoMoreDocuments(err) || driver.IsNotFound(err) {
			return nil, t.ErrNotFound
		}
		return nil, err
	}
	return &cred, nil
}

// Todo: CredGetAll returns credential records for the given user and method, validated only or all.
func (a *adapter) CredGetAll(uid t.Uid, method string, validatedOnly bool) ([]t.Credential, error) {
	query := "for d in credentials \n"
	query = query + "filter d.User == \"" + uid.String() + "\""
	if method != "" {
		query = query + " && d.Method == \"" + method + "\""
	}
	if validatedOnly {
		query = query + " && d.Done == true"
	} else {
		query = query + " && d.DeletedAt != null"
	}
	query = query + "\n return d"

	cur, err := a.QueryManyf(query)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var credentials []t.Credential
	for cur.HasMore() {
		out := &t.Credential{}
		_, err := cur.ReadDocument(a.ctx, out)
		if err != nil {
			return nil, err
		}
		credentials = append(credentials, *out)
	}
	return credentials, nil
}

//TODO:
func (a *adapter) CredDel(uid t.Uid, method, value string) error {
	query := "for d in credentials \n"
	query = query + "filter d.User == \"" + uid.String() + "\""
	if method != "" {
		query = query + " && d.Method == \"" + method + "\""
	}
	if value != "" {
		query = query + " && d.Value == \"" + value + "\""
	}
	query = query + "\n remove d in credentials"
	query = query + "\n return d"
	_, err := a.db.Query(a.ctx, query, nil)
	return err
}

// CredConfirm marks given credential as validated.
func (a *adapter) CredConfirm(uid t.Uid, method string) error {
	cred, err := a.CredGetActive(uid, method)
	if err != nil {
		return err
	}

	cred.Done = true
	cred.UpdatedAt = t.TimeNow()
	if _, err = a.CredUpsert(cred); err != nil {
		return err
	}
	_, err = a.collections.credentials.RemoveDocument(a.ctx, uid.String()+":"+cred.Method+":"+cred.Value)
	return nil
}

// TODO: CredFail increments count of failed validation attepmts for the given credentials.
func (a *adapter) CredFail(uid t.Uid, method string) error {
	query := fmt.Sprintf(`for d in credentials
	filter d.User == "%s" && d.DeletedAt != null  && d.Method == "%s" && d.Done == false
	UPDATE d WITH { Retries:d.Retries + 1, UpdatedAt: "%s" } IN credentials
	`, uid.String(), method, time.Now().Format(time.RFC3339))
	_, err := a.db.Query(a.ctx, query, nil)
	return err
}

// Authentication management for the basic authentication scheme

// Todo: AuthGetUniqueRecord returns authentication record for a given unique value i.e. login.
func (a *adapter) AuthGetUniqueRecord(unique string) (t.Uid, auth.Level, []byte, time.Time, error) {
	var record struct {
		UserId  string     `json:"userid"`
		AuthLvl auth.Level `json:"authlvl"`
		Secret  []byte     `json:"secret"`
		Expires time.Time  `json:"expires"`
	}
	_, err := a.collections.auth.ReadDocument(a.ctx, unique, &record)
	return t.ParseUid(record.UserId), record.AuthLvl, record.Secret, record.Expires, err
}

// AuthGetRecord returns authentication record given user ID and method.
func (a *adapter) AuthGetRecord(uid t.Uid, scheme string) (string, auth.Level, []byte, time.Time, error) {
	var record struct {
		Id      string     `json:"_key"`
		UserId  string     `json:"userid"`
		AuthLvl auth.Level `json:"authlvl"`
		Secret  []byte     `json:"secret"`
		Expires time.Time  `json:"expires"`
	}
	if err := a.QueryOnef(`for d in auth
	filter d.userid == "%s" && d.scheme == "%s"
	return d`, &record, uid.String(), scheme); err != nil {
		return "", 0, nil, time.Time{}, t.ErrNotFound
	}
	return record.Id, record.AuthLvl, record.Secret, record.Expires, nil
}

// AuthAddRecord creates new authentication record
func (a *adapter) AuthAddRecord(uid t.Uid, scheme, unique string, authLvl auth.Level, secret []byte, expires time.Time) error {
	authRecord := map[string]interface{}{
		"_key":    unique,
		"userid":  uid.String(),
		"scheme":  scheme,
		"authlvl": authLvl,
		"secret":  secret,
		"expires": expires}
	if _, err := a.collections.auth.CreateDocument(a.ctx, authRecord); err != nil {
		return t.ErrDuplicate
	}
	return nil
}

// AuthDelScheme deletes an existing authentication scheme for the user.
func (a *adapter) AuthDelScheme(uid t.Uid, scheme string) error {
	if err := a.QueryOnef(`for d in auth
	filter d.UserId == "%s" && d.Scheme == %s 
	remove d._key in auth
	return d`, nil, uid.String(), scheme); err != nil {
		return err
	}
	return nil
}

func (a *adapter) authDelAllRecords(ctx context.Context, uid t.Uid) (int, error) {
	if err := a.QueryOnef(`for d in auth
	filter d.UserId == "%s"
	remove d._key in auth
	return d`, nil, uid.String()); err != nil {
		return 0, err
	}
	return 0, nil
}

// AuthDelAllRecords deletes all records of a given user.
func (a *adapter) AuthDelAllRecords(uid t.Uid) (int, error) {
	return a.authDelAllRecords(a.ctx, uid)
}

// AuthUpdRecord modifies an authentication record.
func (a *adapter) AuthUpdRecord(uid t.Uid, scheme, unique string,
	authLvl auth.Level, secret []byte, expires time.Time) error {
	// The primary key is immutable. If '_key' has changed, we have to replace the old record with a new one:
	// 1. Check if '_key' has changed.
	// 2. If not, execute update by '_key'
	// 3. If yes, first insert the new record (it may fail due to dublicate '_key') then delete the old one.
	var err error
	var record struct {
		Unique string `json:"_key"`
	}
	if err := a.QueryOnef(`for d in auth
	filter d.UserId == "%s" && d.Scheme == "%s"
	return d`, &record, uid.String(), scheme); err != nil {
		return err
	}
	if record.Unique == unique {
		upd := map[string]interface{}{
			"AuthLvl": authLvl,
		}
		if len(secret) > 0 {
			upd["Secret"] = secret
		}
		if !expires.IsZero() {
			upd["Expires"] = expires
		}
		_, err = a.collections.auth.UpdateDocument(a.ctx,
			unique,
			upd)
	} else {
		err = a.AuthAddRecord(uid, scheme, unique, authLvl, secret, expires)
		if err == nil {
			a.AuthDelScheme(uid, scheme)
		}
	}

	return err
}

// Topic management
func (a *adapter) undeleteSubscription(sub *t.Subscription) error {
	_, err := a.collections.subscriptions.UpdateDocument(a.ctx,
		sub.Id,
		map[string]interface{}{
			"UpdatedAt": sub.UpdatedAt,
			"CreatedAt": sub.CreatedAt,
			"ModeGiven": sub.ModeGiven,
			"ModeWant":  sub.ModeWant,
		})
	return err
}

// TopicCreate creates a topic
func (a *adapter) TopicCreate(topic *t.Topic) error {
	_, err := a.collections.topics.CreateDocument(a.ctx, &topic)
	return err
}

func (a *adapter) TopicCreateP2P(initiator, invited *t.Subscription) error {
	initiator.Id = initiator.Topic + ":" + initiator.User
	var err error
	if ok, err := a.collections.subscriptions.DocumentExists(a.ctx, initiator.Id); ok && err == nil {
		if err = a.undeleteSubscription(initiator); err != nil {
			return err
		}
	} else {
		_, err = a.collections.subscriptions.CreateDocument(a.ctx, initiator.Id)
	}
	if err != nil {
		return err
	}
	invited.Id = invited.Topic + ":" + invited.User
	_, err = a.collections.subscriptions.CreateDocument(a.ctx, invited)
	if err != nil {
		if !driver.IsConflict(err) {
			return err
		}
		err = a.undeleteSubscription(invited)
		if err != nil {
			return err
		}
	}
	topic := &t.Topic{ObjHeader: t.ObjHeader{Id: initiator.Topic}}
	topic.ObjHeader.MergeTimes(&initiator.ObjHeader)
	topic.TouchedAt = initiator.GetTouchedAt()
	return a.TopicCreate(topic)
}

// TopicGet loads a single topic by name, if it exists. If the topic does not exist the call returns (nil, nil)
func (a *adapter) TopicGet(topic string) (*t.Topic, error) {
	var tpc = new(t.Topic)
	if _, err := a.collections.topics.ReadDocument(a.ctx, topic, tpc); err != nil {
		if driver.IsNotFound(err) {
			err = nil
		}
		return nil, err
	}
	return tpc, nil
}

// TopicsForUser loads user's contact list: p2p and grp topics, except for 'me' & 'fnd' subscriptions.
// Reads and denormalizes Public & Trusted values.
func (a *adapter) TopicsForUser(uid t.Uid, keepDeleted bool, opts *t.QueryOpt) ([]t.Subscription, error) {
	// Fetch user's subscriptions
	query := ` for d in subscriptions
	filter d.User == "` + uid.String() + `"`
	limit := 0
	ims := time.Time{}
	if opts != nil {
		if opts.Topic != "" {
			query = query + "&& d.Topic == \"" + opts.Topic + "\""
		}
		// Apply the limit only when the client does not manage the cache (or cold start).
		// Otherwise have to get all subscriptions and do a manual join with users/topics.
		if opts.IfModifiedSince == nil {
			if opts.Limit > 0 && opts.Limit < a.maxResults {
				limit = opts.Limit
			} else {
				limit = a.maxResults
			}
		} else {
			ims = *opts.IfModifiedSince
		}
	} else {
		limit = a.maxResults
	}
	if !keepDeleted {
		// Filter out rows with defined deletedat
		query = query + " && d.DeletedAt != \"\""
	}
	if limit > 0 {
		query = query + "\nlimit " + strconv.Itoa(limit) + "\n"
	}
	query = query + `
	let out = (d.DeletedAt != "") ? d : unset(d, "DeletedAt")
	return out`

	cur, err := a.QueryManyf(query)
	if err != nil {
		return nil, err
	}
	// Fetch subscriptions. Two queries are needed: users table (me & p2p) and topics table (p2p and grp).
	// Prepare a list of Separate subscriptions to users vs topics
	join := make(map[string]t.Subscription) // Keeping these to make a join with table for .private and .access
	topq := make([]string, 0, 16)
	usrq := make([]string, 0, 16)
	for cur.HasMore() {
		var sub t.Subscription
		if _, err = cur.ReadDocument(a.ctx, &sub); err != nil {
			continue
		}
		tname := sub.Topic
		sub.User = uid.String()
		tcat := t.GetTopicCat(tname)
		if tcat == t.TopicCatMe || tcat == t.TopicCatFnd {
			continue
		} else if tcat == t.TopicCatP2P {
			uid1, uid2, _ := t.ParseP2P(sub.Topic)
			if uid1 == uid {
				usrq = append(usrq, uid2.String())
				sub.SetWith(uid2.UserId())
			} else {
				usrq = append(usrq, uid1.String())
				sub.SetWith(uid1.UserId())
			}
			topq = append(topq, tname)
		} else {
			if tcat == t.TopicCatGrp {
				tname = t.ChnToGrp(tname)
			}
			topq = append(topq, tname)
		}
		join[tname] = sub
	}
	cur.Close()
	if err != nil {
		return nil, err
	}

	var subs []t.Subscription
	if len(join) == 0 {
		return subs, nil
	}

	if len(topq) > 0 {
		// Fetch grp & p2p topics
		query = "for d in topics"
		pq2 := make([]string, len(topq))
		for idx, v := range topq {
			pq2[idx] = "\"" + v + "\""
		}
		query = query + "\n filter POSITION([" + strings.Join(pq2, ",") + "], d._key)"
		if !keepDeleted {
			query = query + " && d.State != \"" + t.StateDeleted.String() + "\""
		}
		if !ims.IsZero() {
			// Use cache timestamp if provided: get newer entries only.
			query = query + fmt.Sprintf(`&& d.TouchedAt > date_timestamp("%s")`, ims.Format(time.RFC3339))
		}
		query = query + "\n"
		query = query + ` let out = (d.TouchedAt != "") ? d : unset(d, "TouchedAt")
		sort d.TouchedAt DESC
		return out`
		if err != nil {
			return nil, err
		}
		for cur.HasMore() {
			var top t.Topic
			if _, err = cur.ReadDocument(a.ctx, &top); err != nil {
				continue
			}
			sub := join[top.Id]
			sub.UpdatedAt = common.SelectLatestTime(sub.UpdatedAt, top.UpdatedAt)
			sub.SetState(top.State)
			sub.SetTouchedAt(top.TouchedAt)
			sub.SetSeqId(top.SeqId)
			if t.GetTopicCat(sub.Topic) == t.TopicCatGrp {
				sub.SetPublic(top.Public)
				sub.SetTrusted(top.Trusted)
			}
			// Put back the updated value of a p2p subsription, will process further below
			join[top.Id] = sub
		}
		cur.Close()
		if err != nil {
			return nil, err
		}
	}

	// Fetch p2p users and join to p2p tables
	if len(usrq) > 0 {
		query = "for d in topics"
		if !keepDeleted {
			query = query + " \n filter d.State != " + strconv.Itoa(int(t.StateDeleted))
		}

		query = query + "\n return d"
		// Ignoring ims: we need all users to get LastSeen and UserAgent.

		cur, err = a.QueryManyf(query)
		if err != nil {
			return nil, err
		}
		for cur.HasMore() {
			var usr2 t.User
			if _, err = cur.ReadDocument(a.ctx, &usr2); err != nil {
				continue
			}
			joinOn := uid.P2PName(t.ParseUid(usr2.Id))
			if sub, ok := join[joinOn]; ok {
				sub.UpdatedAt = common.SelectLatestTime(sub.UpdatedAt, usr2.UpdatedAt)
				sub.SetState(usr2.State)
				sub.SetPublic(usr2.Public)
				sub.SetTrusted(usr2.Trusted)
				sub.SetDefaultAccess(usr2.Access.Auth, usr2.Access.Anon)
				sub.SetLastSeenAndUA(usr2.LastSeen, usr2.UserAgent)
				join[joinOn] = sub
			}
		}
		cur.Close()
		if err != nil {
			return nil, err
		}
	}
	subs = make([]t.Subscription, 0, len(join))
	for _, sub := range join {
		subs = append(subs, sub)
	}
	return common.SelectEarliestUpdatedSubs(subs, opts, a.maxResults), nil
}

// UsersForTopic loads users' subscriptions for a given topic. Public & Trusted are loaded.
func (a *adapter) UsersForTopic(topic string, keepDeleted bool, opts *t.QueryOpt) ([]t.Subscription, error) {
	tcat := t.GetTopicCat(topic)
	// Fetch topic subscribers
	// Fetch all subscribed users. The number of users is not large
	query := "for d in subscriptions"
	query = query + fmt.Sprintf(`
	filter d.Topic == "%s"`, topic)
	if !keepDeleted && tcat != t.TopicCatP2P {
		query = query + `
		filter !(has(d, "DeletedAt") || d.DeletedAt == "" || d.DeletedAt == null)`
	}

	limit := a.maxResults
	var oneUser t.Uid
	if opts != nil {
		// Ignore IfModifiedSince - we must return all entries
		// Those unmodified will be stripped of Public, Trusted & Private.
		if !opts.User.IsZero() {
			if tcat != t.TopicCatP2P {
				query = query + fmt.Sprintf(`  filter d.User == "%s"`, opts.User.String())
			}
			oneUser = opts.User
		}
		if opts.Limit > 0 && opts.Limit < limit {
			limit = opts.Limit
		}
	}
	query = query + " return d"

	cur, err := a.QueryManyf(query)
	if err != nil {
		return nil, err
	}

	// Fetch subscriptions
	var subs []t.Subscription
	join := make(map[string]t.Subscription)
	usrq := make([]interface{}, 0, 16)
	for cur.HasMore() {
		var sub t.Subscription
		if _, err = cur.ReadDocument(a.ctx, &sub); err != nil {
			break
		}
		join[sub.User] = sub
		usrq = append(usrq, sub.User)
	}
	cur.Close()
	if err != nil {
		return nil, err
	}

	if len(usrq) > 0 {
		subs = make([]t.Subscription, 0, len(usrq))

		// Fetch users by a list of subscriptions
		cur, err = a.QueryManyf(`for d in users
		filter d._key == "%s" && d.State != %d
		return d
		`, usrq, t.StateDeleted)
		if err != nil {
			return nil, err
		}

		for cur.HasMore() {
			var usr2 t.User
			if _, err = cur.ReadDocument(a.ctx, &usr2); err != nil {
				break
			}
			if sub, ok := join[usr2.Id]; ok {
				sub.ObjHeader.MergeTimes(&usr2.ObjHeader)
				sub.SetPublic(usr2.Public)
				sub.SetTrusted(usr2.Trusted)
				sub.SetLastSeenAndUA(usr2.LastSeen, usr2.UserAgent)

				subs = append(subs, sub)
			}
		}
		cur.Close()
		if err != nil {
			return nil, err
		}
	}

	if t.GetTopicCat(topic) == t.TopicCatP2P && len(subs) > 0 {
		// Swap public values & lastSeen of P2P topics as expected.
		if len(subs) == 1 {
			// User is deleted. Nothing we can do.
			subs[0].SetPublic(nil)
			subs[0].SetTrusted(nil)
			subs[0].SetLastSeenAndUA(nil, "")
		} else {
			tmp := subs[0].GetPublic()
			subs[0].SetPublic(subs[1].GetPublic())
			subs[1].SetPublic(tmp)

			tmp = subs[0].GetTrusted()
			subs[0].SetTrusted(subs[1].GetTrusted())
			subs[1].SetTrusted(tmp)

			lastSeen := subs[0].GetLastSeen()
			userAgent := subs[0].GetUserAgent()
			subs[0].SetLastSeenAndUA(subs[1].GetLastSeen(), subs[1].GetUserAgent())
			subs[1].SetLastSeenAndUA(lastSeen, userAgent)
		}

		// Remove deleted and unneeded subscriptions
		if !keepDeleted || !oneUser.IsZero() {
			var xsubs []t.Subscription
			for i := range subs {
				if (subs[i].DeletedAt != nil && !keepDeleted) || (!oneUser.IsZero() && subs[i].Uid() != oneUser) {
					continue
				}
				xsubs = append(xsubs, subs[i])
			}
			subs = xsubs
		}
	}

	return subs, nil
}

// OwnTopics loads a slice of topic names where the user is the owner.
func (a *adapter) OwnTopics(uid t.Uid) ([]string, error) {
	//filter := map[string]interface{}{"owner": uid.String(), "state": map[string]interface{}{"$ne": t.StateDeleted}
	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, err
	}
	var names []string
	for cur.HasMore() {
		var res map[string]string
		if _, err = cur.ReadDocument(a.ctx, &res); err != nil {
			break
		}
		names = append(names, res["_key"])
	}
	cur.Close()
	return names, err
}

// ChannelsForUser loads a slice of topic names where the user is a channel reader and notifications (P) are enabled.
func (a *adapter) ChannelsForUser(uid t.Uid) ([]string, error) {
	//	filter := map[string]interface{}{
	//		"user":      uid.String(),
	//		"deletedat": map[string]interface{}{"$exists": false},
	//		"topic":     map[string]interface{}{"$regex": primitive.Regex{Pattern: "^chn"},
	//		"modewant":  map[string]interface{}{"$bitsAllSet": b.A{t.ModePres},
	//		"modegiven": map[string]interface{}{"$bitsAllSet": b.A{t.ModePres}}
	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, err
	}

	var names []string
	for cur.HasMore() {
		var res map[string]string
		if _, err = cur.ReadDocument(a.ctx, &res); err != nil {
			break
		}
		names = append(names, res["topic"])
	}
	cur.Close()

	return names, err
}

// TopicShare creates topic subscriptions
func (a *adapter) TopicShare(subs []*t.Subscription) error {
	// Assign Ids.
	for i := 0; i < len(subs); i++ {
		subs[i].Id = subs[i].Topic + ":" + subs[i].User
	}

	// Subscription could have been marked as deleted (DeletedAt != nil). If it's marked
	// as deleted, unmark by clearing the DeletedAt field of the old subscription and
	// updating times and ModeGiven.
	for _, sub := range subs {
		_, err := a.collections.subscriptions.CreateDocument(a.ctx, sub)
		if err != nil {
			if driver.IsConflict(err) {
				if err = a.undeleteSubscription(sub); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	return nil
}

// TopicDelete deletes topic, subscription, messages
func (a *adapter) TopicDelete(topic string, hard bool) error {
	err := a.subsDelete(a.ctx, map[string]interface{}{"topic": topic}, hard)
	if err != nil {
		return err
	}
	filter := topic
	if hard {
		if err = a.MessageDeleteList(topic, nil); err != nil {
			return err
		}
		// TODO: file counter
		//	if err = a.decFileUseCounter(a.ctx, "topics", filter); err != nil {
		//		return err
		//	}
		_, err = a.collections.topics.RemoveDocument(a.ctx, filter)
	} else {
		_, err = a.collections.topics.UpdateDocument(a.ctx, filter, map[string]interface{}{
			"State":   t.StateDeleted,
			"StateAt": t.TimeNow(),
		})
	}

	return err
}

// TopicUpdateOnMessage increments Topic's or User's SeqId value and updates TouchedAt timestamp.
func (a *adapter) TopicUpdateOnMessage(topic string, msg *t.Message) error {
	return a.topicUpdate(topic, map[string]interface{}{"SeqId": msg.SeqId, "TouchedAt": msg.CreatedAt})
}

// TopicUpdate updates topic record.
func (a *adapter) TopicUpdate(topic string, update map[string]interface{}) error {
	if t, u := update["TouchedAt"], update["UpdatedAt"]; t == nil && u != nil {
		update["TouchedAt"] = u
	}
	return a.topicUpdate(topic, update)
}

// TopicOwnerChange updates topic's owner
func (a *adapter) TopicOwnerChange(topic string, newOwner t.Uid) error {
	return a.topicUpdate(topic, map[string]interface{}{"Owner": newOwner.String()})
}

func (a *adapter) topicUpdate(topic string, update map[string]interface{}) error {
	_, err := a.collections.topics.UpdateDocument(a.ctx,
		topic,
		update)

	return err
}

// Topic subscriptions

// SubscriptionGet reads a subscription of a user to a topic.
func (a *adapter) SubscriptionGet(topic string, user t.Uid, keepDeleted bool) (*t.Subscription, error) {
	sub := new(t.Subscription)
	query := ` for d in subscriptions
	filter d._key == "%s"`
	filter := topic + ":" + user.String()
	if !keepDeleted {
		query = query + " && (d.DeletedAt == null || d.DeletedAt == \"\")"
	}
	query = query + "\n return d"
	err := a.QueryOnef(query, sub, filter)
	if err != nil {
		if driver.IsNoMoreDocuments(err) {
			return nil, nil
		}
		return nil, err
	}

	return sub, nil
}

// SubsForUser loads all subscriptions of a given user. It does NOT load Public, Trusted or Private values,
// does not load deleted subs.
func (a *adapter) SubsForUser(user t.Uid) ([]t.Subscription, error) {
	//filter := map[string]interface{}{"user": user.String(), "deletedat": map[string]interface{}{"$exists": false}

	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var subs []t.Subscription
	for cur.HasMore() {
		var ss t.Subscription
		if _, err := cur.ReadDocument(a.ctx, &ss); err != nil {
			return nil, err
		}
		ss.Private = nil
		subs = append(subs, ss)
	}
	return subs, err
}

// SubsForTopic gets a list of subscriptions to a given topic. Does NOT load Public & Trusted values.
func (a *adapter) SubsForTopic(topic string, keepDeleted bool, opts *t.QueryOpt) ([]t.Subscription, error) {
	filter := map[string]interface{}{"topic": topic}
	if !keepDeleted {
		filter["deletedat"] = map[string]interface{}{"$exists": false}
	}

	limit := a.maxResults
	if opts != nil {
		// Ignore IfModifiedSince - we must return all entries
		// Those unmodified will be stripped of Public, Trusted & Private.

		if !opts.User.IsZero() {
			filter["user"] = opts.User.String()
		}
		if opts.Limit > 0 && opts.Limit < limit {
			limit = opts.Limit
		}
	}

	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var subs []t.Subscription
	for cur.HasMore() {
		var ss t.Subscription
		if _, err := cur.ReadDocument(a.ctx, &ss); err != nil {
			return nil, err
		}
		subs = append(subs, ss)
	}

	return subs, err
}

//SubsUpdate updates pasrt of a subscription object. Pass nil for fields which don't need to be updated
func (a *adapter) SubsUpdate(topic string, user t.Uid, update map[string]interface{}) error {
	// to get round the hardcoded pass of "Private" key
	query := "	for g in subscriptions"
	if !user.IsZero() {
		query = query + fmt.Sprintf(`	filter g._key == "%s"`, topic+":"+user.String())
	} else {
		query = query + fmt.Sprintf(`	filter g.Topic == "%s"`, topic)
	}
	query = query + "	return g"
	cur, err := a.QueryManyf(query)
	if err != nil {
		return err
	}
	defer cur.Close()
	for cur.HasMore() {
		doc := new(t.Subscription)
		_, err = cur.ReadDocument(a.ctx, doc)
		if err != nil {
			return err
		}
		_, err = a.collections.subscriptions.UpdateDocument(a.ctx, doc.Id, update)
		if err != nil {
			return err
		}
	}
	return nil
}

//SubsDelete deletes a single subscription
func (a *adapter) SubsDelete(topic string, user t.Uid) error {
	var err error
	if a.useTransactions {
		tid, err := a.db.BeginTransaction(a.ctx, driver.TransactionCollections{
			Write: []string{"dellog", "messages"},
		}, &driver.BeginTransactionOptions{LockTimeout: 10 * time.Second})
		if err != nil {
			return err
		}
		defer a.db.CommitTransaction(a.ctx, tid, nil)
	}
	_, err = a.collections.subscriptions.RemoveDocument(a.ctx, topic+":"+user.String())
	if err != nil {
		return err
	}
	if err = a.QueryOnef(`for d in dellog
			filter d.Topic == "%s" && d.DeletedFor == "%s"
			remove d in dellog
			`, nil, topic, user.String()); err != nil {
		return err
	}
	if err = a.QueryOnef(`for d in messages
			filter d.Topic == "%s" && d.DeletedFor == "%s"
			remove d in messages
			`, nil, topic, user.String()); err != nil {
		return err
	}
	return nil
}

// TODO:
// Delete/mark deleted subscriptions.
func (a *adapter) subsDelete(ctx context.Context, filter map[string]interface{}, hard bool) error {
	var err error
	if hard {
		//	_, err = a.collections.subscriptions.RemoveDocuments(ctx, filter)
	} else {
		//now := t.TimeNow()
		//_, err = a.collections.subscriptions.UpdateDocuments(ctx, filter,
		//	map[string]interface{}{"updatedat": now, "deletedat": now})
	}
	return err
}

// TODO:
// Search
// FindUsers searches for new contacts given a list of tags
func (a *adapter) FindUsers(uid t.Uid, req [][]string, opt []string) ([]t.Subscription, error) {
	cur, err := a.QueryManyf("")
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var subs []t.Subscription
	for cur.HasMore() {
		var user t.User
		var sub t.Subscription
		if _, err = cur.ReadDocument(a.ctx, &user); err != nil {
			return nil, err
		}
		if user.Id == uid.String() {
			// Skip the caller
			continue
		}
		sub.CreatedAt = user.CreatedAt
		sub.UpdatedAt = user.UpdatedAt
		sub.User = user.Id
		sub.SetPublic(user.Public)
		sub.SetTrusted(user.Trusted)
		sub.SetDefaultAccess(user.Access.Auth, user.Access.Anon)
		tags := make([]string, 0, 1)
		for _, tag := range user.Tags {
			tags = append(tags, tag)
		}
		sub.Private = tags
		subs = append(subs, sub)
	}

	return subs, nil
}

// TODO:
// FindTopics searches for group topics given a list of tags
func (a *adapter) FindTopics(req [][]string, opt []string) ([]t.Subscription, error) {
	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var subs []t.Subscription
	for cur.HasMore() {
		var topic t.Topic
		var sub t.Subscription
		if _, err = cur.ReadDocument(a.ctx, &topic); err != nil {
			return nil, err
		}

		sub.CreatedAt = topic.CreatedAt
		sub.UpdatedAt = topic.UpdatedAt
		if topic.UseBt {
			sub.Topic = t.GrpToChn(topic.Id)
		} else {
			sub.Topic = topic.Id
		}
		sub.SetPublic(topic.Public)
		sub.SetTrusted(topic.Trusted)
		sub.SetDefaultAccess(topic.Access.Auth, topic.Access.Anon)
		tags := make([]string, 0, 1)
		for _, tag := range topic.Tags {
			tags = append(tags, tag)
		}
		sub.Private = tags
		subs = append(subs, sub)
	}

	return subs, nil
}

// Messages

// MessageSave saves message to database
func (a *adapter) MessageSave(msg *t.Message) error {
	_, err := a.collections.messages.CreateDocument(a.ctx, msg)
	return err
}

// MessageGetAll returns messages matching the query
func (a *adapter) MessageGetAll(topic string, forUser t.Uid, opts *t.QueryOpt) ([]t.Message, error) {
	var limit = a.maxMessageResults
	var lower, upper int
	if opts != nil {
		if opts.Since > 0 {
			lower = opts.Since
		}
		if opts.Before > 0 {
			upper = opts.Before
		}

		if opts.Limit > 0 && opts.Limit < limit {
			limit = opts.Limit
		}
	}
	query := `
	for d in messages
	sort d.Topic, d.SeqId desc
	filter d.Topic == "%s" && (d.DeletedAt == "" || d.DeletedAt == null)
	filter "%s" not in d.DeletedFor[*].User
	filter d.SeqId >= ` + strconv.Itoa(lower)

	if upper != 0 {
		query = query + " && d.SeqId < " + strconv.Itoa(upper)
	}
	if limit > 0 {
		query = query + "\n limit " + strconv.Itoa(limit)
	}
	query = query + "\n return d"
	//fmt.Printf(query, topic, forUser.String())
	cur, err := a.QueryManyf(query, topic, forUser.String())
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var msgs []t.Message
	for cur.HasMore() {
		var msg t.Message
		if _, err = cur.ReadDocument(a.ctx, &msg); err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func (a *adapter) messagesHardDelete(topic string) error {
	var err error

	// TODO: handle file uploads
	filter := map[string]interface{}{"topic": topic}
	//	if _, err = a.collections.dellog.RemoveDocuments(a.ctx, filter); err != nil {
	//		return err
	//	}
	//
	//	if _, err = a.collections.messages.RemoveDocuments(a.ctx, filter); err != nil {
	//		return err
	//	}

	if err = a.decFileUseCounter(a.ctx, "messages", filter); err != nil {
		return err
	}

	return err
}

// MessageDeleteList marks messages as deleted.
// Soft- or Hard- is defined by forUser value: forUSer.IsZero == true is hard.
func (a *adapter) MessageDeleteList(topic string, toDel *t.DelMessage) error {
	var err error

	if toDel == nil {
		// No filter: delete all messages.
		return a.messagesHardDelete(topic)
	}

	// Only some messages are being deleted
	// Start with making a log entry
	_, err = a.collections.dellog.CreateDocument(a.ctx, toDel)
	if err != nil {
		return err
	}
	//	filter := map[string]interface{}{
	//		"topic": topic,
	//		// Skip already hard-deleted messages.
	//		"delid": map[string]interface{}{"$exists": false},
	//	}
	if toDel.DeletedFor == "" {
		// TODO:
		//		if err = a.decFileUseCounter(a.ctx, "messages", filter); err != nil {
		//			return err
		//		}
		//		// Hard-delete individual messages. Message is not deleted but all fields with content
		//		// are replaced with nulls.
		//		_, _, err = a.collections.messages.UpdateDocuments(a.ctx, filter, map[string]interface{}{
		//			"deletedat":   t.TimeNow(),
		//			"delid":       toDel.DelId,
		//			"from":        "",
		//			"head":        nil,
		//			"content":     nil,
		//			"attachments": nil})
	} else {
		// Soft-deleting: adding DelId to DeletedFor

		// Skip messages already soft-deleted for the current user
		//	filter["Deletedfor.user"] = map[string]interface{}{"$ne": toDel.DeletedFor}
		//_, err = a.collections.messages.UpdateDocuments(a.ctx, filter,
		//			map[string]interface{}{"$addToSet": map[string]interface{}{
		//				"deletedfor": &t.SoftDelete{
		//					User:  toDel.DeletedFor,
		//					DelId: toDel.DelId,
		//				})
	}

	// If operation has failed, remove dellog record.
	if err != nil {
		_, _ = a.collections.dellog.RemoveDocument(a.ctx, toDel.Id)
	}
	return err
}

// MessageGetDeleted returns a list of deleted message Ids.
func (a *adapter) MessageGetDeleted(topic string, forUser t.Uid, opts *t.QueryOpt) ([]t.DelMessage, error) {
	var limit = a.maxResults
	var lower, upper int
	if opts != nil {
		if opts.Since > 0 {
			lower = opts.Since
		}
		if opts.Before > 0 {
			upper = opts.Before
		}
		if opts.Limit > 0 && opts.Limit < limit {
			limit = opts.Limit
		}
	}
	filter := map[string]interface{}{
		"topic": topic,
	}
	if upper == 0 {
		filter["delid"] = map[string]interface{}{"$gte": lower}
	} else {
		filter["delid"] = map[string]interface{}{"$gte": lower, "$lt": upper}
	}
	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var dmsgs []t.DelMessage

	return dmsgs, nil
}

// Devices (for push notifications).

// DeviceUpsert creates or updates a device record.
func (a *adapter) DeviceUpsert(uid t.Uid, dev *t.DeviceDef) error {
	userId := uid.String()
	var user t.User
	q1 := `for d in users
	filter d._key == "%s" && d.Devices["%s"].DeviceId == "%s"
	return d`
	err := a.QueryOnef(q1, &user, userId, dev.DeviceId, dev.DeviceId)
	if err == nil && user.Id != "" { // current user owns this device
		// ArrayFilter used to avoid adding another (duplicate) device object. Update that device data
		query := `for d in users
		filter d._key == "%s"
		UPDATE d WITH { Devices: {
			"%s": {
				DeviceId: "%s",
				Platform: "%s",
				LastSeen: "%s",
				Lang: "%s"
			}
		}} IN users
		`
		err = a.QueryOnef(query, nil, userId, dev.DeviceId, dev.DeviceId, dev.Platform, dev.LastSeen.Format(time.RFC3339), dev.Lang)
		return err
	} else if err == t.ErrNotFound { // device is free or owned by other user
		return a.deviceInsert(userId, dev)
	}
	if err != nil {
		return err
	}
	return nil
}

// deviceInsert adds device object to user.devices array
func (a *adapter) deviceInsert(userId string, dev *t.DeviceDef) error {
	filter := userId
	_, err := a.collections.users.UpdateDocument(a.ctx, filter, map[string]interface{}{"Devices": map[string]interface{}{
		dev.DeviceId: dev,
	}})
	return err
}

// DeviceGetAll returns all devices for a given set of users
func (a *adapter) DeviceGetAll(uids ...t.Uid) (map[t.Uid][]t.DeviceDef, int, error) {
	ids := make([]interface{}, len(uids))
	for i, id := range uids {
		ids[i] = id.String()
	}

	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, 0, err
	}
	defer cur.Close()

	result := make(map[t.Uid][]t.DeviceDef)
	count := 0
	var uid t.Uid
	for cur.HasMore() {
		var row struct {
			Id      string `json:"_key"`
			Devices []t.DeviceDef
		}
		if _, err = cur.ReadDocument(a.ctx, &row); err != nil {
			return nil, 0, err
		}
		if row.Devices != nil && len(row.Devices) > 0 {
			if err := uid.UnmarshalText([]byte(row.Id)); err != nil {
				continue
			}

			result[uid] = row.Devices
			count++
		}
	}
	return result, count, nil
}

// DeviceDelete deletes a device record (push token).
func (a *adapter) DeviceDelete(uid t.Uid, deviceID string) error {
	var err error
	filter := uid.String()
	update := map[string]interface{}{}
	if deviceID == "" {
		update["$set"] = map[string]interface{}{"devices": []interface{}{}}
	} else {
		update["$pull"] = map[string]interface{}{"devices": map[string]interface{}{"deviceid": deviceID}}
	}
	_, err = a.collections.users.UpdateDocument(a.ctx, filter, update)
	return err
}

// File upload records. The files are stored outside of the database.

// FileStartUpload initializes a file upload
func (a *adapter) FileStartUpload(fd *t.FileDef) error {
	_, err := a.collections.fileuploads.CreateDocument(a.ctx, fd)
	return err
}

// FileFinishUpload marks file upload as completed, successfully or otherwise.
func (a *adapter) FileFinishUpload(fd *t.FileDef, success bool, size int64) (*t.FileDef, error) {
	if success {
		// Mark upload as completed.
		if _, err := a.collections.fileuploads.UpdateDocument(a.ctx,
			fd.Id,
			map[string]interface{}{
				"UpdatedAt": t.TimeNow(),
				"Status":    t.UploadCompleted,
				"Size":      size,
			}); err != nil {

			return nil, err
		}
		fd.Status = t.UploadCompleted
		fd.Size = size
	} else {
		// Remove record: it's now useless.
		if _, err := a.collections.fileuploads.RemoveDocument(a.ctx, fd.Id); err != nil {
			return nil, err
		}
		fd.Status = t.UploadFailed
		fd.Size = 0
	}

	fd.UpdatedAt = t.TimeNow()

	return fd, nil
}

// FileGet fetches a record of a specific file
func (a *adapter) FileGet(fid string) (*t.FileDef, error) {
	var fd t.FileDef
	_, err := a.collections.fileuploads.ReadDocument(a.ctx, fid, &fd)
	if err != nil {
		return nil, err
	}

	return &fd, nil
}

// FileDeleteUnused deletes records where UseCount is zero. If olderThan is non-zero, deletes
// unused records with UpdatedAt before olderThan.
// Returns array of FileDef.Location of deleted filerecords so actual files can be deleted too.
func (a *adapter) FileDeleteUnused(olderThan time.Time, limit int) ([]string, error) {
	//filter := map[string]interface{}{
	//	map[string]interface{}{"usecount": 0},
	//}
	//	if !olderThan.IsZero() {
	//		filter["Updatedat"] = map[string]interface{}{"$lt": olderThan}
	//	}
	//	if limit > 0 {
	//		findOpts.SetLimit(int64(limit))
	//	}

	cur, err := a.QueryManyf(``)
	if err != nil {
		return nil, err
	}
	defer cur.Close()

	var locations []string
	for cur.HasMore() {
		var result map[string]string
		if _, err := cur.ReadDocument(a.ctx, &result); err != nil {
			return nil, err
		}
		locations = append(locations, result["location"])
	}

	//_, _, err = a.collections.fileuploads.RemoveDocuments(a.ctx, filter)
	return locations, err
}

// TODO: Given a filter query against 'messages' collection, decrement corresponding use counter in 'fileuploads' table.
func (a *adapter) decFileUseCounter(ctx context.Context, collection string, msgFilter map[string]interface{}) error {
	return nil
}

// TODO: FileLinkAttachments connects given topic or message to the file record IDs from the list.
func (a *adapter) FileLinkAttachments(topic string, userId, msgId t.Uid, fids []string) error {
	return nil
}

func (a *adapter) isDbInitialized() bool {
	var result map[string]int
	if _, err := a.collections.kvmeta.ReadDocument(a.ctx, "version", &result); err != nil {
		return false
	}
	return true
}

// GetTestAdapter returns an adapter object. It's required for running tests.
func GetTestAdapter() *adapter {
	return &adapter{}
}

func init() {
	store.RegisterAdapter(&adapter{})
}
