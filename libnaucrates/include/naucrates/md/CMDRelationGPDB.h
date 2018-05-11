//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDRelationGPDB.h
//
//	@doc:
//		Class representing MD relations
//---------------------------------------------------------------------------



#ifndef GPMD_CMDRelationGPDB_H
#define GPMD_CMDRelationGPDB_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDRelation.h"
#include "naucrates/md/IMDColumn.h"

#include "naucrates/md/CMDColumn.h"
#include "naucrates/md/CMDName.h"

namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;


	//---------------------------------------------------------------------------
	//	@class:
	//		CMDRelationGPDB
	//
	//	@doc:
	//		Class representing MD relations
	//
	//---------------------------------------------------------------------------
	class CMDRelationGPDB : public IMDRelation
	{		
		private:
			// memory pool
			IMemoryPool *m_memory_pool;

			// DXL for object
			const CWStringDynamic *m_dxl_str;
			
			// relation mdid
			IMDId *m_mdid;
			
			// table name
			CMDName *m_mdname;
			
			// is this a temporary relation
			BOOL m_is_temp_table;
			
			// storage type
			Erelstoragetype m_rel_storage_type;
			
			// distribution policy
			Ereldistrpolicy m_rel_distr_policy;
			
			// columns
			MDColumnPtrArray *m_md_col_array;
			
			// number of dropped columns
			ULONG m_dropped_cols;
			
			// indices of distribution columns
			ULongPtrArray *m_distr_col_array;
			
			// do we need to consider a hash distributed table as random distributed
			BOOL m_convert_hash_to_random;

			// indices of partition columns
			ULongPtrArray *m_partition_cols_array;
			
			// partition types
			CharPtrArray *m_str_part_types_array;

			// number of partition
			ULONG m_num_of_partitions;

			// array of key sets
			ULongPtrArray2D *m_keyset_array;

			// array of index info
			MDIndexInfoPtrArray *m_mdindex_info_array;

			// array of trigger ids
			MdidPtrArray *m_mdid_trigger_array;

			// array of check constraint mdids
			MdidPtrArray *m_mdid_check_constraint_array;

			// partition constraint
			IMDPartConstraint *m_mdpart_constraint;

			// does this table have oids
			BOOL m_has_oids;

			// number of system columns
			ULONG m_system_columns;
			
			// mapping of column position to positions excluding dropped columns
			UlongUlongHashMap *m_colpos_nondrop_colpos_map;
		
			// mapping of attribute number in the system catalog to the positions of
			// the non dropped column in the metadata object
			IntUlongHashMap *m_attrno_nondrop_col_pos_map;

			// the original positions of all the non-dropped columns
			ULongPtrArray *m_nondrop_col_pos_array;

			// array of column widths including dropped columns
			DrgPdouble *m_col_width_array;

			// private copy ctor
			CMDRelationGPDB(const CMDRelationGPDB &);
		
		public:
			
			// ctor
			CMDRelationGPDB
				(
				IMemoryPool *memory_pool,
				IMDId *mdid,
				CMDName *mdname,
				BOOL is_temp_table,
				Erelstoragetype rel_storage_type, 
				Ereldistrpolicy rel_distr_policy,
				MDColumnPtrArray *mdcol_array,
				ULongPtrArray *distr_col_array,
				ULongPtrArray *partition_cols_array,
				CharPtrArray *str_part_types_array,
				ULONG num_of_partitions,
				BOOL convert_hash_to_random,
				ULongPtrArray2D *keyset_array,
				MDIndexInfoPtrArray *md_index_info_array,
				MdidPtrArray *mdid_triggers_array,
				MdidPtrArray *mdid_check_constraint_array,
				IMDPartConstraint *mdpart_constraint,
				BOOL has_oids
				);
			
			// dtor
			virtual
			~CMDRelationGPDB();
			
			// accessors
			virtual 
			const CWStringDynamic *GetStrRepr() const
			{
				return m_dxl_str;
			}	
			
			// the metadata id
			virtual 
			IMDId *MDId() const;
			
			// relation name
			virtual 
			CMDName Mdname() const;
			
			// is this a temp relation
			virtual 
			BOOL IsTemporary() const;
			
			// storage type (heap, appendonly, ...)
			virtual 
			Erelstoragetype GetRelStorageType() const; 
			
			// distribution policy (none, hash, random)
			virtual 
			Ereldistrpolicy GetRelDistribution() const; 
			
			// number of columns
			virtual 
			ULONG ColumnCount() const;

			// width of a column with regards to the position
			virtual
			DOUBLE ColWidth(ULONG pos) const;

			// does relation have dropped columns
			virtual
			BOOL HasDroppedColumns() const; 
			
			// number of non-dropped columns
			virtual 
			ULONG NonDroppedColsCount() const; 
			
			// return the absolute position of the given attribute position excluding dropped columns
			virtual 
			ULONG NonDroppedColAt(ULONG pos) const;
			
			// return the position of a column in the metadata object given the attribute number in the system catalog
			virtual
			ULONG GetPosFromAttno(INT attno) const;

			// return the original positions of all the non-dropped columns
			virtual
			ULongPtrArray *NonDroppedColsArray() const;

			// number of system columns
			virtual
			ULONG SystemColumnsCount() const;

			// retrieve the column at the given position
			virtual 
			const IMDColumn *GetMdCol(ULONG pos) const;
			
			// number of key sets
			virtual
			ULONG KeySetCount() const;
			
			// key set at given position
			virtual
			const ULongPtrArray *KeySetAt(ULONG pos) const;
			
			// number of distribution columns
			virtual 
			ULONG DistrColumnCount() const;
			
			// retrieve the column at the given position in the distribution columns list for the relation
			virtual 
			const IMDColumn *GetDistrColAt(ULONG pos) const;
			
			// return true if a hash distributed table needs to be considered as random
			virtual 
			BOOL ConvertHashToRandom() const;
			
			// does this table have oids
			virtual
			BOOL HasOids() const;

			// is this a partitioned table
			virtual
			BOOL IsPartitioned() const;
			
			// number of partition keys
			virtual
			ULONG PartColumnCount() const;
			
			// number of partitions
			virtual
			ULONG PartitionCount() const;

			// retrieve the partition key column at the given position
			virtual 
			const IMDColumn *PartColAt(ULONG pos) const;

			// retrieve list of partition types
			virtual
			CharPtrArray *GetPartitionTypes() const;

			// retrieve the partition type of the given level
			virtual
			CHAR PartTypeAtLevel(ULONG ulLevel) const;

			// number of indices
			virtual 
			ULONG IndexCount() const;
			
			// number of triggers
			virtual
			ULONG TriggerCount() const;

			// retrieve the id of the metadata cache index at the given position
			virtual 
			IMDId *IndexMDidAt(ULONG pos) const;

			// check if index is partial given its mdid
			virtual
			BOOL IsPartialIndex(IMDId *mdid) const;

			// retrieve the id of the metadata cache trigger at the given position
			virtual
			IMDId *TriggerMDidAt(ULONG pos) const;

			// serialize metadata relation in DXL format given a serializer object
			virtual 
			void Serialize(gpdxl::CXMLSerializer *) const;

			// number of check constraints
			virtual
			ULONG CheckConstraintCount() const;

			// retrieve the id of the check constraint cache at the given position
			virtual
			IMDId *CheckConstraintMDidAt(ULONG pos) const;

			// part constraint
			virtual
			IMDPartConstraint *MDPartConstraint() const;

#ifdef GPOS_DEBUG
			// debug print of the metadata relation
			virtual 
			void DebugPrint(IOstream &os) const;
#endif
	};
}



#endif // !GPMD_CMDRelationGPDB_H

// EOF
