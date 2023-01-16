package scheduler

import (
	"github.com/case-management-suite/common/config"
	"github.com/case-management-suite/common/service"
	"github.com/case-management-suite/queue"
	"go.uber.org/fx"
)

type WorkSchedulerParams struct {
	fx.In
	AppConfig config.AppConfig
	Utils     service.ServiceUtils
}

type RulesServiceClientResult struct {
	fx.Out
	WorkSchedulerService WorkScheduler
}

func NewWorkSchedulerModule(params WorkSchedulerParams) RulesServiceClientResult {
	newQueueService := queue.QueueServiceFactory(config.RabbitMQ)
	qs := newQueueService(params.AppConfig.RulesServiceConfig.QueueConfig, params.Utils)
	client := NewWorkScheduler(params.AppConfig, qs, params.Utils)
	return RulesServiceClientResult{WorkSchedulerService: client}
}

var WorkSchedulerModule = fx.Module("work_scheduler_service",
	fx.Provide(
		NewWorkSchedulerModule,
	),
)
