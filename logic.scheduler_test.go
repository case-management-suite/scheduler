package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/case-management-suite/common/config"
	common "github.com/case-management-suite/common/config"
	"github.com/case-management-suite/models"
	"github.com/case-management-suite/queue"
	"github.com/case-management-suite/scheduler"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func SetupTest(stub bool) scheduler.WorkScheduler {
	appConfig := common.NewLocalTestAppConfig()
	if stub {
		queueService := queue.QueueServiceFactory(config.GoChannels)(appConfig.RulesServiceConfig.QueueConfig, appConfig.LogConfig)

		return scheduler.NewWorkScheduler(queueService, common.NewLocalTestAppConfig())
	} else {
		ch1 := uuid.NewString()
		ch2 := uuid.NewString()

		newQueueService := queue.QueueServiceFactory(config.RabbitMQ)
		queueService := newQueueService(appConfig.RulesServiceConfig.QueueConfig, appConfig.LogConfig)
		builder := scheduler.NewWorkSchedulerBuilder(queueService, appConfig)
		builder.WithCaseActionsChannel(ch1)
		builder.WithCaseNotificationsChannel(ch2)
		return builder.Build()
	}
}

const timeout = time.Second * 30

func TestScheduleWorkIntegration(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	service := SetupTest(false)
	ExecuteTest(service, t)
}

func TestScheduleWork(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	service := SetupTest(true)
	ExecuteTest(service, t)

}

func ExecuteTest(service scheduler.WorkScheduler, t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// evchan := make(chan models.CaseAction)
	tchan := make(chan bool)
	echan := make(chan error)

	id := "id1"
	action := "someAction"
	service.Start(ctx)
	go func() {
		service.ListenForCaseActions(func(caseAction models.CaseAction) error {
			go func() {
				if caseAction.ID == id {
					log.Error().Str("expected", id).Str("actual", caseAction.CaseRecordID).Msg("CaseRecordID is wrong")
					echan <- errors.New("The CaseRecordID was set to the case action model not to the case record")
				}
				if caseAction.CaseRecordID != id {
					log.Error().Str("expected", id).Str("actual", caseAction.CaseRecordID).Msg("CaseRecordID is wrong")
					echan <- errors.New("CaseRecordID is wrong")
				}
				if caseAction.Action != action {
					log.Error().Str("expected", action).Str("actual", caseAction.Action).Msg("Action is wrong")
					echan <- errors.New("Action is wrong")
				}
				tchan <- true
			}()
			return nil
		}, ctx)
	}()

	service.ExecuteCaseAction(models.CaseRecord{ID: id}, action, ctx)

	select {
	case res := <-tchan:
		fmt.Println(res)
	case fail := <-echan:
		log.Error().Err(fail).Msg("Test failyre")
		t.FailNow()
	case <-time.After(timeout):
		fmt.Println("Test failed due a timeout")
		t.FailNow()
	}
}
