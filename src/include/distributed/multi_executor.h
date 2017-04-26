/*-------------------------------------------------------------------------
 *
 * multi_executor.h
 *	  Executor support for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_EXECUTOR_H
#define MULTI_EXECUTOR_H

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"

#include "distributed/insert_select_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"


#define EXEC_FLAG_CITUS_INSERT_SELECT 0x400


#if (PG_VERSION_NUM >= 90600)
#define tuplecount_t uint64
#else
#define tuplecount_t long
#endif


typedef struct CitusScanState
{
	CustomScanState customScanState;  /* underlying custom scan node */
	MultiPlan *multiPlan;             /* distributed execution plan */
	MultiExecutorType executorType;   /* distributed executor type */
	bool finishedRemoteScan;          /* flag to check if remote scan is finished */
	Tuplestorestate *tuplestorestate; /* tuple store to store distributed results */
} CitusScanState;


extern void multi_ExecutorStart(QueryDesc *queryDesc, int eflags);
extern void multi_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
							  tuplecount_t count);
extern Node * RealTimeCreateScan(CustomScan *scan);
extern Node * TaskTrackerCreateScan(CustomScan *scan);
extern Node * RouterCreateScan(CustomScan *scan);
extern Node * LocalInsertSelectCreateScan(CustomScan *scan);
extern Node * DelayedErrorCreateScan(CustomScan *scan);
extern void CitusSelectBeginScan(CustomScanState *node, EState *estate, int eflags);
extern TupleTableSlot * RealTimeExecScan(CustomScanState *node);
extern TupleTableSlot * TaskTrackerExecScan(CustomScanState *node);
extern void CitusEndScan(CustomScanState *node);
extern void CitusReScan(CustomScanState *node);
extern void CitusExplainScan(CustomScanState *node, List *ancestors, struct
							 ExplainState *es);
extern TupleTableSlot * ReturnTupleFromTuplestore(CitusScanState *scanState);


#endif /* MULTI_EXECUTOR_H */
