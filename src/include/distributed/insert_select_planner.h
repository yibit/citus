/*-------------------------------------------------------------------------
 *
 * insert_select_planner.h
 *
 * Declarations for public functions and types related to planning
 * INSERT..SELECT commands.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_INSERT_SELECT_PLANNER_H
#define MULTI_INSERT_SELECT_PLANNER_H


#include "postgres.h"

#include "distributed/multi_physical_planner.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"


extern MultiPlan * CreateLocalInsertSelectPlan(Query *originalQuery);
extern bool IsLocalInsertSelectPlan(PlannedStmt *planStatement);


#endif /* MULTI_INSERT_SELECT_PLANNER_H */
