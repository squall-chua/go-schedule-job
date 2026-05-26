module github.com/squall-chua/go-schedule-job/codec/proto

go 1.25.1

require (
	github.com/squall-chua/go-schedule-job v0.0.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
)

replace github.com/squall-chua/go-schedule-job => ../..
