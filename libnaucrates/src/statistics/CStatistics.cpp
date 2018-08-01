//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CStatistics.cpp
//
//	@doc:
//		Histograms based statistics
//---------------------------------------------------------------------------

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CScaleFactorUtils.h"

#include "naucrates/statistics/CJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftOuterJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CLeftAntiSemiJoinStatsProcessor.h"
#include "naucrates/statistics/CInnerJoinStatsProcessor.h"
#include "gpos/common/CBitSet.h"
#include "gpos/sync/CAutoMutex.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CReqdPropRelational.h"
#include "gpopt/mdcache/CMDAccessor.h"


#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpmd;
using namespace gpdxl;
using namespace gpopt;

// default number of rows in relation
const CDouble CStatistics::DefaultRelationRows(1000.0);

// epsilon to be used for various computations
const CDouble CStatistics::Epsilon(0.001);

// minimum number of rows in relation
const CDouble CStatistics::MinRows(1.0);

// default column width
const CDouble CStatistics::DefaultColumnWidth(8.0);

// default number of distinct values
const CDouble CStatistics::DefaultDistinctValues(1000.0);

// the default m_bytearray_value for operators that have no cardinality estimation risk
const ULONG CStatistics::no_card_est_risk_default_val = 1;

// ctor
CStatistics::CStatistics
	(
	IMemoryPool *memory_pool,
	UlongHistogramHashMap *col_histogram_mapping,
	UlongDoubleHashMap *col_id_width_mapping,
	CDouble rows,
	BOOL is_empty,
	ULONG num_predicates
	)
	:
	m_colid_histogram_mapping(col_histogram_mapping),
	m_colid_width_mapping(col_id_width_mapping),
	m_rows(rows),
	m_stats_estimation_risk(no_card_est_risk_default_val),
	m_empty(is_empty),
	m_num_rebinds(1.0), // by default, a stats object is rebound to parameters only once
	m_num_predicates(num_predicates),
	m_src_upper_bound_NDVs(NULL)
{
	GPOS_ASSERT(NULL != m_colid_histogram_mapping);
	GPOS_ASSERT(NULL != m_colid_width_mapping);
	GPOS_ASSERT(CDouble(0.0) <= m_rows);

	// hash map for source id -> max source cardinality mapping
	m_src_upper_bound_NDVs = GPOS_NEW(memory_pool) UpperBoundNDVPtrArray(memory_pool);

	m_stats_conf = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig()->GetStatsConf();
}

// Dtor
CStatistics::~CStatistics()
{
	m_colid_histogram_mapping->Release();
	m_colid_width_mapping->Release();
	m_src_upper_bound_NDVs->Release();
}

// look up the width of a particular column
const CDouble *
CStatistics::GetWidth
	(
	ULONG col_id
	)
	const
{
	return m_colid_width_mapping->Find(&col_id);
}


//	cap the total number of distinct values (NDVs) in buckets to the number of rows
void
CStatistics::CapNDVs
	(
	CDouble rows,
	UlongHistogramHashMap *col_histogram_mapping
	)
{
	GPOS_ASSERT(NULL != col_histogram_mapping);
	UlongHistogramHashMapIter col_hist_mapping(col_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		CHistogram *histogram = const_cast<CHistogram *>(col_hist_mapping.Value());
		histogram->CapNDVs(rows);
	}
}

// helper print function
IOstream &
CStatistics::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "{" << std::endl;
	os << "Rows = " << Rows() << std::endl;
	os << "Rebinds = " << NumRebinds() << std::endl;

	UlongHistogramHashMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG col_id = *(col_hist_mapping.Key());
		os << "Col" << col_id << ":" << std::endl;
		const CHistogram *histogram = col_hist_mapping.Value();
		histogram->OsPrint(os);
		os << std::endl;
	}

	UlongDoubleHashMapIter col_width_map_iterator(m_colid_width_mapping);
	while (col_width_map_iterator.Advance())
	{
		ULONG col_id = *(col_width_map_iterator.Key());
		os << "Col" << col_id << ":" << std::endl;
		const CDouble *width = col_width_map_iterator.Value();
		os << " width " << (*width) << std::endl;
	}

	const ULONG length = m_src_upper_bound_NDVs->Size();
	for (ULONG i = 0; i < length; i++)
	{
		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		upper_bound_NDVs->OsPrint(os);
	}
	os << "StatsEstimationRisk = " << StatsEstimationRisk() << std::endl;
	os << "}" << std::endl;

	return os;
}

//	return the total number of rows for this statistics object
CDouble
CStatistics::Rows() const
{
	return m_rows;
}

// return the estimated skew of the given column
CDouble
CStatistics::GetSkew
	(
	ULONG col_id
	)
	const
{
	CHistogram *histogram = m_colid_histogram_mapping->Find(&col_id);
	if (NULL == histogram)
	{
		return CDouble(1.0);
	}

	return histogram->GetSkew();
}

// return total width in bytes
CDouble
CStatistics::Width() const
{
	CDouble total_width(0.0);
	UlongDoubleHashMapIter col_width_map_iterator(m_colid_width_mapping);
	while (col_width_map_iterator.Advance())
	{
		const CDouble *width = col_width_map_iterator.Value();
		total_width = total_width + (*width);
	}
	return total_width.Ceil();
}

// return the width in bytes of a set of columns
CDouble
CStatistics::Width
	(
	ULongPtrArray *col_ids
	)
	const
{
	GPOS_ASSERT(NULL != col_ids);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CDouble total_width(0.0);
	const ULONG size = col_ids->Size();
	for (ULONG idx = 0; idx < size; idx++)
	{
		ULONG col_id = *((*col_ids)[idx]);
		CDouble *width = m_colid_width_mapping->Find(&col_id);
		if (NULL != width)
		{
			total_width = total_width + (*width);
		}
		else
		{
			CColRef *colref = col_factory->LookupColRef(col_id);
			GPOS_ASSERT(NULL != colref);

			total_width = total_width + CStatisticsUtils::DefaultColumnWidth(colref->Pmdtype());
		}
	}
	return total_width.Ceil();
}

// return width in bytes of a set of columns
CDouble
CStatistics::Width
	(
	IMemoryPool *memory_pool,
	CColRefSet *colrefs
	)
	const
{
	GPOS_ASSERT(NULL != colrefs);

	ULongPtrArray *col_ids = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);
	colrefs->ExtractColIds(memory_pool, col_ids);

	CDouble width = Width(col_ids);
	col_ids->Release();

	return width;
}

// return dummy statistics object
CStatistics *
CStatistics::MakeDummyStats
	(
	IMemoryPool *memory_pool,
	ULongPtrArray *col_ids,
	CDouble rows
	)
{
	GPOS_ASSERT(NULL != col_ids);

	// hash map from colid -> histogram for resultant structure
	UlongHistogramHashMap *col_histogram_mapping = GPOS_NEW(memory_pool) UlongHistogramHashMap(memory_pool);

	// hashmap from colid -> width (double)
	UlongDoubleHashMap *col_id_width_mapping = GPOS_NEW(memory_pool) UlongDoubleHashMap(memory_pool);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	BOOL is_empty = (CStatistics::Epsilon >= rows);
	CHistogram::AddDummyHistogramAndWidthInfo(memory_pool, col_factory, col_histogram_mapping, col_id_width_mapping, col_ids, is_empty);

	CStatistics *stats = GPOS_NEW(memory_pool) CStatistics(memory_pool, col_histogram_mapping, col_id_width_mapping, rows, is_empty);
	CreateAndInsertUpperBoundNDVs(memory_pool, stats, col_ids, rows);

	return stats;
}

// add upper bound ndvs information for a given set of columns
void
CStatistics::CreateAndInsertUpperBoundNDVs
	(
	IMemoryPool *memory_pool,
	CStatistics *stats,
	ULongPtrArray *col_ids,
	CDouble rows
)
{
	GPOS_ASSERT(NULL != stats);
	GPOS_ASSERT(NULL != col_ids);

	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRefSet *colrefs = GPOS_NEW(memory_pool) CColRefSet(memory_pool);
	const ULONG num_cols = col_ids->Size();
	for (ULONG i = 0; i < num_cols; i++)
	{
		ULONG col_id = *(*col_ids)[i];
		const CColRef *colref = col_factory->LookupColRef(col_id);
		if (NULL != colref)
		{
			colrefs->Include(colref);
		}
	}

	if (0 < colrefs->Size())
	{
		stats->AddCardUpperBound(GPOS_NEW(memory_pool) CUpperBoundNDVs(colrefs, rows));
	}
	else
	{
		colrefs->Release();
	}
}

//	return dummy statistics object
CStatistics *
CStatistics::MakeDummyStats
	(
	IMemoryPool *memory_pool,
	ULongPtrArray *col_histogram_mapping,
	ULongPtrArray *col_width_mapping,
	CDouble rows
	)
{
	GPOS_ASSERT(NULL != col_histogram_mapping);
	GPOS_ASSERT(NULL != col_width_mapping);

	BOOL is_empty = (CStatistics::Epsilon >= rows);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	// hash map from colid -> histogram for resultant structure
	UlongHistogramHashMap *result_col_histogram_mapping = GPOS_NEW(memory_pool) UlongHistogramHashMap(memory_pool);

	const ULONG num_col_hist = col_histogram_mapping->Size();
	for (ULONG ul = 0; ul < num_col_hist; ul++)
	{
		ULONG col_id = *(*col_histogram_mapping)[ul];

		CColRef *colref = col_factory->LookupColRef(col_id);
		GPOS_ASSERT(NULL != colref);

		// empty histogram
		CHistogram *histogram = CHistogram::MakeDefaultHistogram(memory_pool, colref, is_empty);
		result_col_histogram_mapping->Insert(GPOS_NEW(memory_pool) ULONG(col_id), histogram);
	}

	// hashmap from colid -> width (double)
	UlongDoubleHashMap *colid_width_mapping = GPOS_NEW(memory_pool) UlongDoubleHashMap(memory_pool);

	const ULONG num_col_width = col_width_mapping->Size();
	for (ULONG ul = 0; ul < num_col_width; ul++)
	{
		ULONG col_id = *(*col_width_mapping)[ul];

		CColRef *colref = col_factory->LookupColRef(col_id);
		GPOS_ASSERT(NULL != colref);

		CDouble width = CStatisticsUtils::DefaultColumnWidth(colref->Pmdtype());
		colid_width_mapping->Insert(GPOS_NEW(memory_pool) ULONG(col_id), GPOS_NEW(memory_pool) CDouble(width));
	}

	CStatistics *stats = GPOS_NEW(memory_pool) CStatistics(memory_pool, result_col_histogram_mapping, colid_width_mapping, rows, false /* is_empty */);
	CreateAndInsertUpperBoundNDVs(memory_pool, stats, col_histogram_mapping, rows);

	return stats;
}


//	check if the input statistics from join statistics computation empty
BOOL
CStatistics::IsEmptyJoin
	(
	const CStatistics *outer_stats,
	const CStatistics *inner_side_stats,
	BOOL IsLASJ
	)
{
	GPOS_ASSERT(NULL != outer_stats);
	GPOS_ASSERT(NULL != inner_side_stats);

	if (IsLASJ)
	{
		return outer_stats->IsEmpty();
	}

	return outer_stats->IsEmpty() || inner_side_stats->IsEmpty();
}

// Currently, Pstats[Join type] are thin wrappers the C[Join type]StatsProcessor class's method
// for deriving the stat objects for the corresponding join operator

//	return statistics object after performing LOJ operation with another statistics structure
CStatistics *
CStatistics::CalcLOJoinStats
	(
	IMemoryPool *memory_pool,
	const IStatistics *other_stats,
	StatsPredJoinArray *join_preds_stats
	)
	const
{
	return CLeftOuterJoinStatsProcessor::CalcLOJoinStatsStatic(memory_pool, this, other_stats, join_preds_stats);
}



//	return statistics object after performing semi-join with another statistics structure
CStatistics *
CStatistics::CalcLSJoinStats
	(
	IMemoryPool *memory_pool,
	const IStatistics *inner_side_stats,
	StatsPredJoinArray *join_preds_stats
	)
	const
{
	return CLeftSemiJoinStatsProcessor::CalcLSJoinStatsStatic(memory_pool, this, inner_side_stats, join_preds_stats);
}



// return statistics object after performing inner join
CStatistics *
CStatistics::CalcInnerJoinStats
	(
	IMemoryPool *memory_pool,
	const IStatistics *other_stats,
	StatsPredJoinArray *join_preds_stats
	)
	const
{
	return CInnerJoinStatsProcessor::CalcInnerJoinStatsStatic(memory_pool, this, other_stats, join_preds_stats);
}

// return statistics object after performing LASJ
CStatistics *
CStatistics::CalcLASJoinStats
	(
	IMemoryPool *memory_pool,
	const IStatistics *other_stats,
	StatsPredJoinArray *join_preds_stats,
	BOOL DoIgnoreLASJHistComputation
	)
	const
{
	return CLeftAntiSemiJoinStatsProcessor::CalcLASJoinStatsStatic(memory_pool, this, other_stats, join_preds_stats, DoIgnoreLASJHistComputation);
}

//	helper method to copy statistics on columns that are not excluded by bitset
void
CStatistics::AddNotExcludedHistograms
	(
	IMemoryPool *memory_pool,
	CBitSet *excluded_cols,
	UlongHistogramHashMap *col_histogram_mapping
	)
	const
{
	GPOS_ASSERT(NULL != excluded_cols);
	GPOS_ASSERT(NULL != col_histogram_mapping);

	UlongHistogramHashMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG col_id = *(col_hist_mapping.Key());
		if (!excluded_cols->Get(col_id))
		{
			const CHistogram *histogram = col_hist_mapping.Value();
			CStatisticsUtils::AddHistogram(memory_pool, col_id, histogram, col_histogram_mapping);
		}

		GPOS_CHECK_ABORT;
	}
}

UlongDoubleHashMap *
CStatistics::CopyWidths
	(
	IMemoryPool *memory_pool
	)
	const
{
	UlongDoubleHashMap *widths_copy = GPOS_NEW(memory_pool) UlongDoubleHashMap(memory_pool);
	CStatisticsUtils::AddWidthInfo(memory_pool, m_colid_width_mapping, widths_copy);

	return widths_copy;
}

void
CStatistics::CopyWidthsInto
		(
				IMemoryPool *memory_pool,
				UlongDoubleHashMap *col_id_width_mapping
		)
	const
{
	CStatisticsUtils::AddWidthInfo(memory_pool, m_colid_width_mapping, col_id_width_mapping);
}

UlongHistogramHashMap *
CStatistics::CopyHistograms
	(
	IMemoryPool *memory_pool
	)
	const
{
	// create hash map from colid -> histogram for resultant structure
	UlongHistogramHashMap *histograms_copy = GPOS_NEW(memory_pool) UlongHistogramHashMap(memory_pool);

	BOOL is_empty = IsEmpty();

	UlongHistogramHashMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG col_id = *(col_hist_mapping.Key());
		const CHistogram *histogram = col_hist_mapping.Value();
		CHistogram *histogram_copy = NULL;
		if (is_empty)
		{
			histogram_copy =  GPOS_NEW(memory_pool) CHistogram(GPOS_NEW(memory_pool) BucketArray(memory_pool), false /* is_well_defined */);
		}
		else
		{
			histogram_copy = histogram->CopyHistogram(memory_pool);
		}

		histograms_copy->Insert(GPOS_NEW(memory_pool) ULONG(col_id), histogram_copy);
	}

	return histograms_copy;
}



//	return required props associated with statistics object
CReqdPropRelational *
CStatistics::GetReqdRelationalProps
	(
	IMemoryPool *memory_pool
	)
	const
{
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
	GPOS_ASSERT(NULL != col_factory);

	CColRefSet *colrefs = GPOS_NEW(memory_pool) CColRefSet(memory_pool);

	// add columns from histogram map
	UlongHistogramHashMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG col_id = *(col_hist_mapping.Key());
		CColRef *colref = col_factory->LookupColRef(col_id);
		GPOS_ASSERT(NULL != colref);

		colrefs->Include(colref);
	}

	return GPOS_NEW(memory_pool) CReqdPropRelational(colrefs);
}

// append given statistics to current object
void
CStatistics::AppendStats
	(
	IMemoryPool *memory_pool,
	IStatistics *input_stats
	)
{
	CStatistics *stats = CStatistics::CastStats(input_stats);

	CHistogram::AddHistograms(memory_pool, stats->m_colid_histogram_mapping, m_colid_histogram_mapping);
	GPOS_CHECK_ABORT;

	CStatisticsUtils::AddWidthInfo(memory_pool, stats->m_colid_width_mapping, m_colid_width_mapping);
	GPOS_CHECK_ABORT;
}

// copy statistics object
IStatistics *
CStatistics::CopyStats
	(
	IMemoryPool *memory_pool
	)
	const
{
	return ScaleStats(memory_pool, CDouble(1.0) /*factor*/);
}

// return a copy of this statistics object scaled by a given factor
IStatistics *
CStatistics::ScaleStats
	(
	IMemoryPool *memory_pool,
	CDouble factor
	)
	const
{
	UlongHistogramHashMap *histograms_new = GPOS_NEW(memory_pool) UlongHistogramHashMap(memory_pool);
	UlongDoubleHashMap *widths_new = GPOS_NEW(memory_pool) UlongDoubleHashMap(memory_pool);

	CHistogram::AddHistograms(memory_pool, m_colid_histogram_mapping, histograms_new);
	GPOS_CHECK_ABORT;

	CStatisticsUtils::AddWidthInfo(memory_pool, m_colid_width_mapping, widths_new);
	GPOS_CHECK_ABORT;

	CDouble scaled_num_rows = m_rows * factor;

	// create a scaled stats object
	CStatistics *scaled_stats = GPOS_NEW(memory_pool) CStatistics
												(
												memory_pool,
												histograms_new,
												widths_new,
												scaled_num_rows,
												IsEmpty(),
												m_num_predicates
												);

	// In the output statistics object, the upper bound source cardinality of the scaled column
	// cannot be greater than the the upper bound source cardinality information maintained in the input
	// statistics object. Therefore we choose CStatistics::EcbmMin the bounding method which takes
	// the minimum of the cardinality upper bound of the source column (in the input hash map)
	// and estimated output cardinality.

	// modify source id to upper bound card information
	CStatisticsUtils::ComputeCardUpperBounds(memory_pool, this, scaled_stats, scaled_num_rows, CStatistics::EcbmMin /* card_bounding_method */);

	return scaled_stats;
}

//	copy statistics object with re-mapped column ids
IStatistics *
CStatistics::CopyStatsWithRemap
	(
	IMemoryPool *memory_pool,
	UlongColRefHashMap *colref_mapping,
	BOOL must_exist
	)
	const
{
	GPOS_ASSERT(NULL != colref_mapping);
	UlongHistogramHashMap *histograms_new = GPOS_NEW(memory_pool) UlongHistogramHashMap(memory_pool);
	UlongDoubleHashMap *widths_new = GPOS_NEW(memory_pool) UlongDoubleHashMap(memory_pool);

	AddHistogramsWithRemap(memory_pool, m_colid_histogram_mapping, histograms_new, colref_mapping, must_exist);
	AddWidthInfoWithRemap(memory_pool, m_colid_width_mapping, widths_new, colref_mapping, must_exist);

	// create a copy of the stats object
	CStatistics *stats_copy = GPOS_NEW(memory_pool) CStatistics
											(
											memory_pool,
											histograms_new,
											widths_new,
											m_rows,
											IsEmpty(),
											m_num_predicates
											);

	// In the output statistics object, the upper bound source cardinality of the join column
	// cannot be greater than the the upper bound source cardinality information maintained in the input
	// statistics object.

	// copy the upper bound ndv information
	const ULONG length = m_src_upper_bound_NDVs->Size();
	for (ULONG i = 0; i < length; i++)
	{
		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		CUpperBoundNDVs *upper_bound_NDVs_copy = upper_bound_NDVs->CopyUpperBoundNDVWithRemap(memory_pool, colref_mapping);

		if (NULL != upper_bound_NDVs_copy)
	 	{
			stats_copy->AddCardUpperBound(upper_bound_NDVs_copy);
	 	}
	}

	return stats_copy;
}

//	return the column identifiers of all columns whose statistics are
//	maintained by the statistics object
ULongPtrArray *
CStatistics::GetColIdsWithStats
	(
	IMemoryPool *memory_pool
	)
	const
{
	ULongPtrArray *col_ids = GPOS_NEW(memory_pool) ULongPtrArray(memory_pool);

	UlongHistogramHashMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG col_id = *(col_hist_mapping.Key());
		col_ids->Append(GPOS_NEW(memory_pool) ULONG(col_id));
	}

	return col_ids;
}

// return the set of column references we have statistics for
CColRefSet *
CStatistics::GetColRefSet
	(
	IMemoryPool *memory_pool
	)
	const
{
	CColRefSet *colrefs = GPOS_NEW(memory_pool) CColRefSet(memory_pool);
	CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();

	UlongHistogramHashMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG col_id = *(col_hist_mapping.Key());
		CColRef *colref = col_factory->LookupColRef(col_id);
		GPOS_ASSERT(NULL != colref);

		colrefs->Include(colref);
	}

	return colrefs;
}

//	append given histograms to current object where the column ids have been re-mapped
void
CStatistics::AddHistogramsWithRemap
	(
	IMemoryPool *memory_pool,
	UlongHistogramHashMap *src_histograms,
	UlongHistogramHashMap *dest_histograms,
	UlongColRefHashMap *colref_mapping,
	BOOL
#ifdef GPOS_DEBUG
	must_exist
#endif //GPOS_DEBUG
	)
{
	UlongColRefHashMapIter colref_iterator(colref_mapping);
	while (colref_iterator.Advance())
	{
		ULONG src_col_id = *(colref_iterator.Key());
		const CColRef *dest_colref = colref_iterator.Value();
		GPOS_ASSERT_IMP(must_exist, NULL != dest_colref);

		ULONG dest_col_id = dest_colref->Id();

		const CHistogram *src_histogram = src_histograms->Find(&src_col_id);
		if (NULL != src_histogram)
		{
			CStatisticsUtils::AddHistogram(memory_pool, dest_col_id, src_histogram, dest_histograms);
		}
	}
}

// add width information where the column ids have been re-mapped
void
CStatistics::AddWidthInfoWithRemap
		(
		IMemoryPool *memory_pool,
		UlongDoubleHashMap *src_width,
		UlongDoubleHashMap *dest_width,
		UlongColRefHashMap *colref_mapping,
		BOOL must_exist
		)
{
	UlongDoubleHashMapIter col_width_map_iterator(src_width);
	while (col_width_map_iterator.Advance())
	{
		ULONG col_id = *(col_width_map_iterator.Key());
		CColRef *new_colref = colref_mapping->Find(&col_id);
		if (must_exist && NULL == new_colref)
		{
			continue;
		}

		if (NULL != new_colref)
		{
			col_id = new_colref->Id();
		}

		if (NULL == dest_width->Find(&col_id))
		{
			const CDouble *width = col_width_map_iterator.Value();
			CDouble *width_copy = GPOS_NEW(memory_pool) CDouble(*width);
#ifdef GPOS_DEBUG
			BOOL result =
#endif // GPOS_DEBUG
					dest_width->Insert(GPOS_NEW(memory_pool) ULONG(col_id), width_copy);
			GPOS_ASSERT(result);
		}
	}
}

// return the index of the array of upper bound ndvs to which column reference belongs
ULONG
CStatistics::GetIndexUpperBoundNDVs
	(
	const CColRef *colref
	)
{
	GPOS_ASSERT(NULL != colref);
 	CAutoMutex am(m_src_upper_bound_mapping_mutex);
 	am.Lock();

 	const ULONG length = m_src_upper_bound_NDVs->Size();
 	for (ULONG i = 0; i < length; i++)
 	{
 		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		if (upper_bound_NDVs->IsPresent(colref))
 	 	{
 	 		return i;
 	 	}
	}

	return gpos::ulong_max;
}

// add upper bound of source cardinality
void
CStatistics::AddCardUpperBound
	(
	CUpperBoundNDVs *upper_bound_NDVs
	)
{
	GPOS_ASSERT(NULL != upper_bound_NDVs);

	CAutoMutex am(m_src_upper_bound_mapping_mutex);
	am.Lock();

	m_src_upper_bound_NDVs->Append(upper_bound_NDVs);
}

// return the dxl representation of the statistics object
CDXLStatsDerivedRelation *
CStatistics::GetDxlStatsDrvdRelation
	(
	IMemoryPool *memory_pool,
	CMDAccessor *md_accessor
	)
	const
{
	DXLStatsDerivedColArray *dxl_stats_derived_col_array = GPOS_NEW(memory_pool) DXLStatsDerivedColArray(memory_pool);

	UlongHistogramHashMapIter col_hist_mapping(m_colid_histogram_mapping);
	while (col_hist_mapping.Advance())
	{
		ULONG col_id = *(col_hist_mapping.Key());
		const CHistogram *histogram = col_hist_mapping.Value();

		CDouble *width = m_colid_width_mapping->Find(&col_id);
		GPOS_ASSERT(width);

		CDXLStatsDerivedColumn *dxl_derived_col_stats = histogram->TranslateToDXLDerivedColumnStats(memory_pool, md_accessor, col_id, *width);
		dxl_stats_derived_col_array->Append(dxl_derived_col_stats);
	}

	return GPOS_NEW(memory_pool) CDXLStatsDerivedRelation(m_rows, IsEmpty(), dxl_stats_derived_col_array);
}

// return the upper bound of ndvs for a column reference
CDouble
CStatistics::GetColUpperBoundNDVs
	(
	const CColRef *colref
	)
{
	GPOS_ASSERT(NULL != colref);

	CAutoMutex am(m_src_upper_bound_mapping_mutex);
	am.Lock();

	const ULONG length = m_src_upper_bound_NDVs->Size();
	for (ULONG i = 0; i < length; i++)
	{
		const CUpperBoundNDVs *upper_bound_NDVs = (*m_src_upper_bound_NDVs)[i];
		if (upper_bound_NDVs->IsPresent(colref))
		{
			return upper_bound_NDVs->UpperBoundNDVs();
		}
	}

	return DefaultDistinctValues;
}


// look up the number of distinct values of a particular column
CDouble
CStatistics::GetNDVs
	(
	const CColRef *colref
	)
{
	ULONG col_id = colref->Id();
	CHistogram *col_histogram = m_colid_histogram_mapping->Find(&col_id);
	if (NULL != col_histogram)
	{
		return std::min(col_histogram->GetNumDistinct(), GetColUpperBoundNDVs(colref));
	}

#ifdef GPOS_DEBUG
	{
		// the case of no histogram available for requested column signals
		// something wrong with computation of required statistics columns,
		// we print a debug message to log this case

		CAutoMemoryPool amp;
		CAutoTrace at(amp.Pmp());

		at.Os() << "\nREQUESTED NDVs FOR COL (" << colref->Id()  << ") WITH A MISSING HISTOGRAM";
	}
#endif //GPOS_DEBUG

	// if no histogram is available for required column, we use
	// the number of rows as NDVs estimate
	return std::min(m_rows, GetColUpperBoundNDVs(colref));
}


// EOF
