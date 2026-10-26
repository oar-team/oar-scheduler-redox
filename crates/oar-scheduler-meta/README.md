# Crate oar-scheduler-meta

## Overview

Work in progress crate allowing to run the meta-scheduler fully in Rust, including database access.
No Python bindings are required.

The `PlatformTrait` struct of the meta-scheduler contains an instance of `oar_scheduler_db::Session`, allowing it to access the database directly from
Rust.

## Usage

The crate should be compiled as a binary and executed to run the meta-scheduler. Though the meta-scheduler is not fully implemented yet.
