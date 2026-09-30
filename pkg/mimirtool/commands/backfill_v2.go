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
		Action(c.action(logConfig, c.start))

	uploadCmd := cmd.Command("upload", "Upload blocks into an existing backfill job.").
		Action(c.action(logConfig, c.upload))
	uploadCmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)
	uploadCmd.Flag("job", "ID of the backfill job to upload into.").
		Required().
		StringVar(&c.jobID)

	runCmd := cmd.Command("run", "Start a backfill job, upload blocks into it, and finish it if every block uploads successfully.").
		Action(c.action(logConfig, c.run))
	runCmd.Arg("block-dir", "block to upload").Required().SetValue(&c.blocks)

	// TODO: status and cancel subcommands
	// TODO: a way to fetch the ID of an existing job
	finishCmd := cmd.Command("finish", "Finish a backfill job and hand it off for asynchronous processing.").
		Action(c.action(logConfig, c.finishJob))
	finishCmd.Flag("job", "ID of the backfill job to finish.").
		Required().
		StringVar(&c.jobID)
}

func (c *BackfillV2Command) start(ctx context.Context, cli *client.MimirClient, _ log.Logger) error {
	jobID, err := cli.StartBackfillJob(ctx)
	if err != nil {
		return err
	}

	fmt.Println(jobID)
	return nil
}

func (c *BackfillV2Command) upload(ctx context.Context, cli *client.MimirClient, _ log.Logger) error {
	return cli.UploadBackfillBlocks(ctx, c.jobID, c.blocks)
}

func (c *BackfillV2Command) run(ctx context.Context, cli *client.MimirClient, logger log.Logger) error {
	jobID, err := cli.StartBackfillJob(ctx)
	if err != nil {
		return err
	}
	level.Info(logger).Log("msg", "started backfill job", "job", jobID)

	if err := cli.UploadBackfillBlocks(ctx, jobID, c.blocks); err != nil {
		return err
	}

	return finishBackfillJob(ctx, cli, logger, jobID)
}

func (c *BackfillV2Command) finishJob(ctx context.Context, cli *client.MimirClient, logger log.Logger) error {
	return finishBackfillJob(ctx, cli, logger, c.jobID)
}

func finishBackfillJob(ctx context.Context, cli *client.MimirClient, logger log.Logger, jobID string) error {
	if err := cli.FinishBackfillJob(ctx, jobID); err != nil {
		return err
	}
	level.Info(logger).Log("msg", "backfill job finished", "job", jobID)
	return nil
}

func (c *BackfillV2Command) action(logConfig *LoggerConfig, action func(context.Context, *client.MimirClient, log.Logger) error) kingpin.Action {
	return func(_ *kingpin.ParseContext) error {
		logger := logConfig.Logger()
		cli, err := client.New(c.clientConfig, logger)
		if err != nil {
			return err
		}

		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		return action(ctx, cli, logger)
	}
}
