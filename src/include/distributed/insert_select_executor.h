/*-------------------------------------------------------------------------
 *
 * insert_select_executor.h
 *
 * Declarations for public functions and types related to executing
 * INSERT..SELECT commands.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_INSERT_SELECT_EXECUTOR_H
#define MULTI_INSERT_SELECT_EXECUTOR_H


#include "executor/execdesc.h"


extern void LocalInsertSelectExecutorStart(QueryDesc *queryDesc, int eflags);


#endif /* MULTI_INSERT_SELECT_EXECUTOR_H */
