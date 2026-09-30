// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type BackfillV2Command struct {
	clientConfig client.Config
	jobID        string
	blocks       blockList
}

func (c *BackfillV2Command) Register(app *kingpin.Application, envVars EnvVarNames, logConfig *LoggerConfig) {
	cmd := app.Command("backfill-v2", "Upload Prometheus TSDB blocks to Grafana Mimir with the v2 backfill API. Blocks are uploaded into a backfill job, and finishing the job hands it off for asynchronous processing.")
	registerBackfillClientFlags(cmd, envVars, &c.clientConfig)

	cmd.Command("start", "Start a new backfill job and print its ID to stdout.").
		Action(withSignalContext(logConfig, c.start))

	uploadCmd := cmd.Command("upload", "Upload blocks into an existing backfill job.").
		Action(withSignalContext(logConfig, c.upload))
	uploadCmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)
	uploadCmd.Flag("job", "ID of the backfill job to upload into.").
		Required().
		StringVar(&c.jobID)

	runCmd := cmd.Command("run", "Start a backfill job, upload blocks into it, and finish it if every block uploads successfully.").
		Action(withSignalContext(logConfig, c.run))
	runCmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)

	// TODO: status and cancel subcommands
	finishCmd := cmd.Command("finish", "Finish a backfill job and hand it off for asynchronous processing.").
		Action(withSignalContext(logConfig, c.finishJob))
	finishCmd.Flag("job", "ID of the backfill job to finish.").
		Required().
		StringVar(&c.jobID)
}

func (c *BackfillV2Command) start(ctx context.Context, logger log.Logger) error {
	cli, err := client.New(c.clientConfig, logger)
	if err != nil {
		return err
	}

	jobID, err := cli.StartBackfillJob(ctx)
	if err != nil {
		return err
	}

	fmt.Println(jobID)
	return nil
}

func (c *BackfillV2Command) upload(ctx context.Context, logger log.Logger) error {
	cli, err := client.New(c.clientConfig, logger)
	if err != nil {
		return err
	}

	return cli.UploadBackfillBlocks(ctx, c.jobID, c.blocks)
}

func (c *BackfillV2Command) run(ctx context.Context, logger log.Logger) error {
	cli, err := client.New(c.clientConfig, logger)
	if err != nil {
		return err
	}

	jobID, err := cli.StartBackfillJob(ctx)
	if err != nil {
		return err
	}
	level.Info(logger).Log("msg", "started backfill job", "job", jobID)

	if err := cli.UploadBackfillBlocks(ctx, jobID, c.blocks); err != nil {
		return err
	}

	if err := cli.FinishBackfillJob(ctx, jobID); err != nil {
		return err
	}
	level.Info(logger).Log("msg", "backfill job finished", "job", jobID)
	return nil
}

func (c *BackfillV2Command) finishJob(ctx context.Context, logger log.Logger) error {
	cli, err := client.New(c.clientConfig, logger)
	if err != nil {
		return err
	}

	if err := cli.FinishBackfillJob(ctx, c.jobID); err != nil {
		return err
	}
	level.Info(logger).Log("msg", "backfill job finished", "job", c.jobID)
	return nil
}

func withSignalContext(logConfig *LoggerConfig, action func(context.Context, log.Logger) error) kingpin.Action {
	return func(_ *kingpin.ParseContext) error {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		return action(ctx, logConfig.Logger())
	}
}
