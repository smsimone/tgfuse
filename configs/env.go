package configs

type Database string

var (
	MONGO Database = "mongo"
	ETCD  Database = "etcd"
)

type DBConfig interface {
	GetURL() string
}

type MongoConfig struct {
	URL      string
	Port     string
	Username string
	Password string
}

type EtcdConfig struct {
	URL string
}

func (e EtcdConfig) GetURL() string {
	return e.URL
}

func (m MongoConfig) GetURL() string {
	return m.URL
}

var (
	CHUNK_SIZE            = 20000000 // bytes
	TG_BOT_TOKEN          = "<BOT_TOKEN>"
	TG_CHAT_ID            = "17369111"
	LOG_FILE              = "tgfuse.log" // "/var/log/tgfuse/tgfuse.log"
	GC_DELAY              = 4            // seconds
	CHUNK_TTL             = 5 * 60       // seconds
	DB_CONFIG    DBConfig = &EtcdConfig{
		URL: "89.168.16.172:2379",
	}
)
