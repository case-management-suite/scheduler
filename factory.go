package scheduler

import (
	"fmt"
	"reflect"

	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/common/factory"
	"github.com/case-management-suite/common/service"
	"github.com/case-management-suite/queue"
	queue_api "github.com/case-management-suite/queue/api"
)

type WorkSchedulerFactory func(config.AppConfig, queue_api.QueueService, service.ServiceUtils) WorkScheduler

type WorkSchedulerFactories struct {
	factory.FactorySet
	WorkSchedulerFactory WorkSchedulerFactory
	QueueServiceFactory  queue.QueueServiceConstructor
	ServiceUtilsFactory  func() service.ServiceUtils
}

func (f WorkSchedulerFactories) BuildWorkScheduler(appConfig config.AppConfig) (*WorkScheduler, error) {
	if err := factory.ValidateFactorySet(f); err != nil {
		return nil, fmt.Errorf("factory: %s -> %w;", reflect.TypeOf(f).Name(), err)
	}
	r := f.WorkSchedulerFactory(
		appConfig,
		f.QueueServiceFactory(
			appConfig.RulesServiceConfig.QueueConfig,
			f.ServiceUtilsFactory(),
		),
		f.ServiceUtilsFactory())
	return &r, nil
}
