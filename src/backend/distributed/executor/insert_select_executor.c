/*-------------------------------------------------------------------------
 *
 * insert_select_executor.c
 *
 * Executor logic for local INSERT..SELECT.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/transaction_management.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"


/*
 * LocalInsertSelectExecutorStart is called from the executor start hook
 * and replaces the query plan with that of the SELECT part of an
 * INSERT..SELECT command, but sets the destination to a CopyDestReceiver
 * such that the results of the SELECT are appended to the distributed
 * table.
 */
void
LocalInsertSelectExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;
	CustomScan *customScan = (CustomScan *) planStatement->planTree;
	MultiPlan *multiPlan = GetMultiPlan(customScan);

	Query *subquery = multiPlan->insertSelectQuery;
	List *insertTargetList = multiPlan->insertTargetList;
	Oid relationId = multiPlan->targetRelationId;

	PlannedStmt *subquerySelectPlan = NULL;
	ListCell *selectTargetCell = NULL;
	ListCell *insertTargetCell = NULL;
	List *columnNameList = NIL;
	EState *executorState = NULL;
	bool stopOnFailure = false;

	CitusCopyDestReceiver *copyDest = NULL;

	/* build a column name list for the DestReceiver */
	forboth(insertTargetCell, insertTargetList,
			selectTargetCell, subquery->targetList)
	{
		TargetEntry *insertTargetEntry = (TargetEntry *) lfirst(insertTargetCell);
		TargetEntry *selectTargetEntry = (TargetEntry *) lfirst(selectTargetCell);

		Var *columnVar = NULL;
		Oid columnType = InvalidOid;
		int32 columnTypeMod = 0;
		Oid selectOutputType = InvalidOid;

		if (!IsA(insertTargetEntry->expr, Var))
		{
			ereport(ERROR, (errmsg("can only handle regular columns in the target list")));
		}

		columnVar = (Var *) insertTargetEntry->expr;
		columnType = get_atttype(relationId, columnVar->varattno);
		columnTypeMod = get_atttypmod(relationId, columnVar->varattno);
		selectOutputType = columnVar->vartype;

		if (columnType != selectOutputType)
		{
			Expr *selectExpression = selectTargetEntry->expr;
			Expr *typeCast = (Expr *) coerce_to_target_type(NULL,
															(Node *) selectExpression,
															selectOutputType,
															columnType,
															columnTypeMod,
															COERCION_EXPLICIT,
															COERCE_IMPLICIT_CAST,
															-1);

			selectTargetEntry->expr = typeCast;
		}

		columnNameList = lappend(columnNameList, insertTargetEntry->resname);
	}

	/*
	 * Replace INSERT..SELECT plan with the SELECT plan.
	 *
	 * We copy the original statement's queryId, to allow
	 * pg_stat_statements and similar extension to associate the
	 * statement with the toplevel statement.
	 */
	subquerySelectPlan = pg_plan_query(subquery, 0, queryDesc->params);
	subquerySelectPlan->queryId = queryDesc->plannedstmt->queryId;
	queryDesc->plannedstmt = subquerySelectPlan;

	executorState = CreateExecutorState();
	executorState->es_top_eflags = eflags;
	executorState->es_instrument = queryDesc->instrument_options;

	BeginOrContinueCoordinatedTransaction();

	/*
	 * Set up a DestReceiver that copies into the distributed table.
	 */
	copyDest = CreateCitusCopyDestReceiver(relationId, columnNameList,
										   executorState, stopOnFailure);

	/* send query results into the shards */
	queryDesc->dest = (DestReceiver *) copyDest;

	/* call ExecutorStart with the new plan tree */
	ExecutorStart(queryDesc, eflags);
}
