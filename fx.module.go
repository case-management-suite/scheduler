package scheduler

import (
	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/queue"
	"go.uber.org/fx"
)

type WorkSchedulerParams struct {
	fx.In
	AppConfig config.AppConfig
}

type RulesServiceClientResult struct {
	fx.Out
	WorkSchedulerService WorkScheduler
}

func NewWorkSchedulerModule(params WorkSchedulerParams) RulesServiceClientResult {
	newQueueService := queue.QueueServiceFactory(config.RabbitMQ)
	qs := newQueueService(params.AppConfig.RulesServiceConfig.QueueConfig, params.AppConfig.LogConfig)
	client := NewWorkScheduler(qs, params.AppConfig)
	return RulesServiceClientResult{WorkSchedulerService: client}
}

var WorkSchedulerModule = fx.Module("work_scheduler_service",
	fx.Provide(
		NewWorkSchedulerModule,
	),
)
