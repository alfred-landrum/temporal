module go.temporal.io/server

go 1.18

require (
	cloud.google.com/go/storage v1.22.0
	github.com/aws/aws-sdk-go v1.43.38
	github.com/blang/semver/v4 v4.0.0
	github.com/brianvoe/gofakeit/v6 v6.15.0
	github.com/cactus/go-statsd-client/statsd v0.0.0-20200423205355-cb0885a1018c
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/emirpasic/gods v1.15.1
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v1.0.0
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang/mock v1.6.0
	github.com/google/uuid v1.3.0
	github.com/iancoleman/strcase v0.2.0
	github.com/jmoiron/sqlx v1.3.4
	github.com/jonboulle/clockwork v0.2.3
	github.com/lib/pq v1.10.5
	github.com/olivere/elastic v6.2.37+incompatible
	github.com/olivere/elastic/v7 v7.0.32
	github.com/pborman/uuid v1.2.1
	github.com/prometheus/client_golang v1.12.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/stretchr/testify v1.7.1
	github.com/temporalio/ringpop-go v0.0.0-20211012191444-6f91b5915e95
	github.com/uber-go/tally/v4 v4.1.1
	github.com/uber/tchannel-go v1.22.3
	github.com/urfave/cli v1.22.5
	github.com/urfave/cli/v2 v2.4.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	go.opentelemetry.io/otel v1.6.3
	go.opentelemetry.io/otel/exporters/prometheus v0.29.0
	go.opentelemetry.io/otel/metric v0.29.0
	go.opentelemetry.io/otel/sdk v1.6.3
	go.opentelemetry.io/otel/sdk/metric v0.29.0
	go.temporal.io/api v1.7.1-0.20220510045017-9181247f5f2f
	go.temporal.io/sdk v1.14.1-0.20220429221638-3a2b86ebed54
	go.temporal.io/version v0.3.0
	go.uber.org/atomic v1.9.0
	go.uber.org/fx v1.17.1
	go.uber.org/multierr v1.8.0
	go.uber.org/zap v1.21.0
	golang.org/x/oauth2 v0.0.0-20220411215720-9780585627b5
	golang.org/x/time v0.0.0-20220411224347-583f2d630306
	google.golang.org/api v0.74.0
	google.golang.org/grpc v1.46.0
	google.golang.org/grpc/examples v0.0.0-20220412212246-3bf6719fc8ab
	gopkg.in/square/go-jose.v2 v2.6.0
	gopkg.in/validator.v2 v2.0.1
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	modernc.org/sqlite v1.16.0
)

require (
	cloud.google.com/go v0.100.2 // indirect
	cloud.google.com/go/compute v1.5.0 // indirect
	cloud.google.com/go/iam v0.3.0 // indirect
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/googleapis/gax-go/v2 v2.3.0 // indirect
	github.com/googleapis/go-type-adapters v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.33.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/twmb/murmur3 v1.1.6 // indirect
	github.com/uber-common/bark v1.3.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/otel/trace v1.6.3 // indirect
	go.uber.org/dig v1.14.1 // indirect
	golang.org/x/crypto v0.0.0-20220411220226-7b82a4e95df4 // indirect
	golang.org/x/mod v0.6.0-dev.0.20220106191415-9b9b3d81d5e3 // indirect
	golang.org/x/net v0.0.0-20220425223048-2871e0cb64e4 // indirect
	golang.org/x/sys v0.0.0-20220503163025-988cb79eb6c6 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.10 // indirect
	golang.org/x/xerrors v0.0.0-20220411194840-2f41105eb62f // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220505152158-f39f71e6c8f3 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.35.26 // indirect
	modernc.org/ccgo/v3 v3.16.2 // indirect
	modernc.org/libc v1.15.0 // indirect
	modernc.org/mathutil v1.4.1 // indirect
	modernc.org/memory v1.0.7 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.1 // indirect
	modernc.org/token v1.0.0 // indirect
)
