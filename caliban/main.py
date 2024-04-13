#!/usr/bin/python
"""Entry point for Caliban's various modes."""

from __future__ import absolute_import, division, print_function

import logging as ll
import sys

from absl import app, logging
from blessings import Terminal

import caliban.cli as cli
import caliban.config as c
import caliban.docker.build as b
import caliban.history.cli
import caliban.platform.cloud.core as cloud
import caliban.platform.cloud.util as cu
import caliban.platform.gke as gke
import caliban.platform.gke.cli
import caliban.platform.notebook as pn
import caliban.platform.run as pr
import caliban.platform.shell as ps
import caliban.util.schema as cs

# Set logging level for main logger
ll.getLogger("caliban.main").setLevel(logging.ERROR)
t = Terminal()


def run_app(arg_input):
    args = vars(arg_input)
    script_args = c.extract_script_args(args)
    command = args["command"]

    if command == "cluster":
        return gke.cli.run_cli_command(args)

    # Resolve job mode and generate docker args
    job_mode = cli.resolve_job_mode(args)
    docker_args = cli.generate_docker_args(job_mode, args)
    docker_run_args = args.get("docker_run_args", [])

    if command == "shell":
        # Run shell command
        mount_home = not args["bare"]
        image_id = args.get("image_id")
        shell = args["shell"]
        ps.run_interactive(
            job_mode,
            image_id=image_id,
            run_args=docker_run_args,
            mount_home=mount_home,
            shell=shell,
            **docker_args,
        )

    elif command == "notebook":
        # Run notebook command
        port = args.get("port")
        lab = args.get("lab")
        version = args.get("jupyter_version")
        mount_home = not args["bare"]
        pn.run_notebook(
            job_mode,
            port=port,
            lab=lab,
            version=version,
            run_args=docker_run_args,
            mount_home=mount_home,
            **docker_args,
        )

    elif command == "build":
        # Build command
        package = args["module"]
        b.build_image(job_mode, package=package, **docker_args)

    elif command == "status":
        # Status command
        caliban.history.cli.get_status(args)

    elif command == "stop":
        # Stop command
        caliban.history.cli.stop(args)

    elif command == "resubmit":
        # Resubmit command
        caliban.history.cli.resubmit(args)

    elif command == "run":
        # Run experiments command
        dry_run = args["dry_run"]
        package = args["module"]
        image_id = args.get("image_id")
        exp_config = args.get("experiment_config")
        xgroup = args.get("xgroup")

        pr.run_experiments(
            job_mode,
            run_args=docker_run_args,
            script_args=script_args,
            image_id=image_id,
            experiment_config=exp_config,
            dry_run=dry_run,
            package=package,
            xgroup=xgroup,
            **docker_args,
        )

    elif command == "cloud":
        # Cloud command
        project_id = c.extract_project_id(args)
        region = c.extract_region(args)
        cloud_key = c.extract_cloud_key(args)

        dry_run = args["dry_run"]
        package = args["module"]
        # Missing continuation here, add your cloud command logic

# Missing continuation here, add your main logic for execution


if __name__ == "__main__":
    app.run(run_app)
