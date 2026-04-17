package app

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/go-to-k/delstack/internal/io"
	"github.com/go-to-k/delstack/internal/operation"
	"github.com/go-to-k/delstack/internal/preprocessor"
)

// IStackExecutor executes the deletion of a single CloudFormation stack.
type IStackExecutor interface {
	Execute(ctx context.Context, stack string, config aws.Config, operatorFactory *operation.OperatorFactory, forceMode bool, isRootStack bool) error
}

type StackExecutor struct {
	ProgressMode bool
}

func (e *StackExecutor) Execute(
	ctx context.Context,
	stack string,
	config aws.Config,
	operatorFactory *operation.OperatorFactory,
	forceMode bool,
	isRootStack bool,
) error {
	operatorCollection := operation.NewOperatorCollection(config, operatorFactory)
	operatorManager := operation.NewOperatorManager(operatorCollection)
	cloudformationStackOperator := operatorFactory.CreateCloudFormationStackOperator()

	io.Logger.Info().Msgf("[%v]: Start deletion. Please wait a few minutes...", stack)

	if forceMode {
		io.Logger.Debug().Msgf("[%v]: RemoveDeletionPolicy: start", stack)
		if err := cloudformationStackOperator.RemoveDeletionPolicy(ctx, aws.String(stack)); err != nil {
			return fmt.Errorf("[%v]: Failed to remove deletion policy: %w", stack, err)
		}
		io.Logger.Debug().Msgf("[%v]: RemoveDeletionPolicy: done", stack)
	}

	io.Logger.Debug().Msgf("[%v]: PreprocessRecursively: start", stack)
	pp := preprocessor.NewRecursivePreprocessorFromConfig(config, forceMode)
	if err := pp.PreprocessRecursively(ctx, aws.String(stack)); err != nil {
		return fmt.Errorf("[%v]: %w", stack, err)
	}
	io.Logger.Debug().Msgf("[%v]: PreprocessRecursively: done", stack)

	io.Logger.Debug().Msgf("[%v]: DeleteCloudFormationStack: start", stack)

	var stopProgress func()
	if e.ProgressMode {
		stopProgress = e.startProgressWatcher(ctx, stack, operatorFactory)
	}

	err := cloudformationStackOperator.DeleteCloudFormationStack(ctx, aws.String(stack), isRootStack, operatorManager)

	if stopProgress != nil {
		stopProgress()
	}

	if err != nil {
		return fmt.Errorf("[%v]: Failed to delete: %w", stack, err)
	}

	io.Logger.Info().Msgf("[%v]: Successfully deleted!!", stack)
	return nil
}

func (e *StackExecutor) startProgressWatcher(
	ctx context.Context,
	stack string,
	operatorFactory *operation.OperatorFactory,
) func() {
	cfnClient := operatorFactory.CreateCloudFormationClient()

	// Best-effort total count; a 0 just renders as "N/0".
	total := 0
	if resources, listErr := cfnClient.ListStackResources(ctx, aws.String(stack)); listErr == nil {
		total = len(resources)
	}

	watcher := operation.NewStackProgressWatcher(stack, cfnClient, total)
	watchCtx, cancel := context.WithCancel(ctx)
	waitDone := watcher.Start(watchCtx)

	return func() {
		cancel()
		waitDone()
	}
}
