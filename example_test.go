package driver_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	driver "github.com/toshicloud/toshi-sandbox-driver"
)

func ExampleConfigFromEnv() {
	cfg, err := driver.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cfg.Project, cfg.Environment, cfg.Name)
}

func ExampleNew() {
	ctx := context.Background()

	cfg, err := driver.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	d, err := driver.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := d.Check(ctx); err != nil {
		log.Fatal(err) // unreachable, refused, or the wrong scope
	}
}

func ExampleDriver_Create() {
	ctx := context.Background()

	cfg, err := driver.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	d, err := driver.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	sb, err := d.Create(ctx, "task-42", driver.Spec{Size: "toshi-linux-2x", MaxDuration: 30 * time.Minute})
	if err != nil {
		log.Fatal(err)
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		log.Fatal(err) // it ended instead, or ctx ended
	}

	res, err := d.Execute(ctx, sb, driver.Command{
		Argv:    []string{"sh", "-c", "make test"},
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Timeout: 10 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	// res.ExitCode, res.Cancelled, res.TimedOut
	fmt.Println(res.ExitCode, res.Cancelled, res.TimedOut)
}

func ExampleDriver_Delete() {
	ctx := context.Background()

	cfg, err := driver.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	d, err := driver.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	sb, err := d.Create(ctx, "task-42", driver.Spec{Size: "toshi-linux-2x", MaxDuration: 30 * time.Minute})
	if err != nil {
		log.Fatal(err)
	}

	op, err := d.Delete(ctx, sb)
	if err != nil {
		log.Fatal(err)
	}
	op, err = d.WaitOperation(ctx, op.ID)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleDriver_Stop() {
	ctx := context.Background()

	cfg, err := driver.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	d, err := driver.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	sb, err := d.Create(ctx, "task-43", driver.Spec{Size: "toshi-linux-2x", MaxDuration: 30 * time.Minute, KeepVM: true})
	if err != nil {
		log.Fatal(err)
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		log.Fatal(err)
	}

	if err := d.Stop(ctx, sb); err != nil {
		log.Fatal(err) // created without KeepVM, or ended
	}
	// sb.State() is driver.StateStopped; Execute fails with driver.ErrStopped.

	if err := d.Start(ctx, sb); err != nil {
		log.Fatal(err)
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		log.Fatal(err)
	}
	res, err := d.Execute(ctx, sb, driver.Command{Argv: []string{"ls"}, Stdout: os.Stdout})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.ExitCode)
}

func ExampleDriver_Recover() {
	ctx := context.Background()

	cfg, err := driver.ConfigFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	d, err := driver.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	stored := "" // the cursor the last run saved

	rec, err := d.Recover(ctx, stored) // stored: the last cursor saved, or ""
	if err != nil {
		log.Fatal(err)
	}
	// rec.Sandboxes, rec.Events, rec.Cursor
	err = d.Watch(ctx, rec.Cursor, func(u driver.Update) error {
		// u.Sandbox changed, or u.Event happened. Handle it, then save
		// u.Cursor if it is set: only events carry one.
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
