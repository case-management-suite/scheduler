package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/common/utils"
	"github.com/case-management-suite/models"
	queue_api "github.com/case-management-suite/queue/api"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type WorkHandler = func(WorkUnit)

type NotificationHandler = func(WorkNotification)

type WorkType = string
type WorkNotificationType = string

const (
	ExecuteCaseAction WorkType = "ExecuteCaseAction"
)

const (
	CaseUpdates WorkNotificationType = "CaseUpdates"
)

type Payload interface {
	ToBytes() []byte
}

type GenericPayload struct{}

func (GenericPayload) ToBytes() []byte {
	return []byte{}
}

type CaseExecutionPayload struct {
	CaseRecord models.CaseRecord
	Action     string
	*GenericPayload
}

type WorkUnit struct {
	Type    WorkType
	Payload Payload
}

type WorkNotification struct {
	Type   WorkNotificationType
	Status string
}

type WorkScheduler interface {
	ExecuteCaseAction(models.CaseRecord, string, context.Context) error
	ListenForCaseActions(func(models.CaseAction) error, context.Context) error
	ListenForCaseUpdates(func(models.CaseRecord) error, context.Context) error
	NotifyCaseUpdate(models.CaseRecord, context.Context) error
	Start(context.Context) error
	Stop() error
}

type WorkSchedulerImpl struct {
	CaseActionsChannel       string
	CastNotificationsChannel string
	Queue                    queue_api.QueueService
	QueueConfig              config.QueueConnectionConfig
	log                      zerolog.Logger
}

func (w WorkSchedulerImpl) ExecuteCaseAction(record models.CaseRecord, action string, ctx context.Context) error {
	id := models.NewCaseActionUUID()
	caseAction := models.CaseAction{ID: id, Action: action, CaseRecord: record, CaseRecordID: record.ID}

	data, err := json.Marshal(caseAction)
	if err != nil {
		log.Debug().Str("record", fmt.Sprintf("%v", record)).Msg("Failed to marshal record into JSON before sending it to the queue")
		return err
	}
	w.log.Debug().Str("UUID", record.ID).Str("service", "scheduler").Msg("Sending work unit to queue")
	w.Queue.Send(ctx, w.CaseActionsChannel, data, 5)

	return nil
}

// func QueueActionHandler(h func(models.CaseAction) error) func(q queue.QueueEvent) error {
// 	return func(q queue.QueueEvent) error {
// 		r := models.CaseAction{}
// 		json.Unmarshal(q, &r)
// 		log.Debug().Str("UUID", r.ID).Str("service", "scheduler").Msg("Received case record from queue")
// 		return h(r)
// 	}
// }

func unmarshalMessage[T models.CaseAction | models.CaseRecord](d queue_api.Delivery) (*T, error) {
	l := new(T)
	if err := json.Unmarshal(d.Body, &l); err != nil {
		d.Nack(true)
		return nil, err
	}
	return l, nil
}

func (w WorkSchedulerImpl) ListenForCaseActions(h func(models.CaseAction) error, ctx context.Context) error {
	out, err := w.Queue.Listen(w.CaseActionsChannel, ctx)
	if err != nil {
		return err
	}
	go connectToChannels(ctx, out, h)
	return nil
}

func connectToChannels[T models.CaseAction | models.CaseRecord](ctx context.Context, out *queue_api.QueueConsumerOutput, fn func(T) error) {
	for {
		select {
		case msg := <-out.Out:
			rec, _ := unmarshalMessage[T](msg)
			fn(*rec)
			continue
		case <-out.Done:
			return
		case <-out.Err:
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
			continue
		}
	}
}

func (w WorkSchedulerImpl) ListenForCaseUpdates(h func(models.CaseRecord) error, ctx context.Context) error {
	out, err := w.Queue.Listen(w.CastNotificationsChannel, ctx)
	if err != nil {
		return err
	}
	go connectToChannels(ctx, out, h)
	return nil
}

func (w WorkSchedulerImpl) NotifyCaseUpdate(record models.CaseRecord, ctx context.Context) error {
	data, err := json.Marshal(record)
	if err != nil {
		w.log.Debug().Str("record", fmt.Sprintf("%v", record)).Msg("Failed to marshal record into JSON before sending it to the queue")
		return err
	}

	return w.Queue.Send(ctx, w.CastNotificationsChannel, data, w.QueueConfig.SendRetries)
}

func (w WorkSchedulerImpl) Start(ctx context.Context) error {
	ctx = utils.DecorateContext(ctx, "WorkScheduler")
	return w.Queue.Connect(ctx, []string{w.CastNotificationsChannel, w.CaseActionsChannel})
}

func (w WorkSchedulerImpl) Stop() error {
	w.Queue.Close()
	return nil
}

type workSchedulerBuilder struct {
	caseActionsChannel       *string
	caseNotificationsChannel *string
	queue                    *queue_api.QueueService
}

func (b *workSchedulerBuilder) WithCaseActionsChannel(ch string) workSchedulerBuilder {
	b.caseActionsChannel = &ch
	return *b
}

func (b *workSchedulerBuilder) WithCaseNotificationsChannel(ch string) workSchedulerBuilder {
	b.caseNotificationsChannel = &ch
	return *b
}

func (b *workSchedulerBuilder) Build() WorkScheduler {
	return WorkSchedulerImpl{Queue: *b.queue, CaseActionsChannel: *b.caseActionsChannel, CastNotificationsChannel: *b.caseNotificationsChannel}
}

func NewWorkSchedulerBuilder(q queue_api.QueueService, appConfig config.AppConfig) workSchedulerBuilder {
	c1 := appConfig.RulesServiceConfig.QueueConfig.CaseActionsChannel
	c2 := appConfig.RulesServiceConfig.QueueConfig.CaseNotificationsChannel
	return workSchedulerBuilder{queue: &q, caseActionsChannel: &c1, caseNotificationsChannel: &c2}
}

func NewWorkScheduler(queue queue_api.QueueService, appConfig config.AppConfig) WorkScheduler {
	conf := appConfig.RulesServiceConfig.QueueConfig
	logConf := appConfig.LogConfig
	logger := logConf.Logger.Level(logConf.WorkScheduler)
	logger.Debug().Str("CaseActionsChannel", conf.CaseActionsChannel).Str("CaseNotificationsChannel", conf.CaseNotificationsChannel).Msg("Initialized Work scheduler")
	return WorkSchedulerImpl{Queue: queue, CaseActionsChannel: conf.CaseActionsChannel, CastNotificationsChannel: conf.CaseNotificationsChannel, log: logger}
}
