//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerWindowSpec.h
//
//	@doc:
//		SAX parse handler class for parsing the window specification
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerWindowSpec_H
#define GPDXL_CParseHandlerWindowSpec_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLWindowSpec.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerWindowSpec
	//
	//	@doc:
	//		SAX parse handler class for parsing the window specification
	//
	//---------------------------------------------------------------------------
	class CParseHandlerWindowSpec : public CParseHandlerBase
	{
		private:

			// array of partition-by column identifiers used by the window functions
			ULongPtrArray *m_part_by_col_identifier_array;

			// window specification generated by the parser
			CDXLWindowSpec *m_dxl_window_spec_gen;

			// name of window specification
			CMDName *m_mdname;

			// does the window spec have a frame definition
			BOOL m_has_window_frame;

			// private copy ctor
			CParseHandlerWindowSpec(const CParseHandlerWindowSpec&);

			// process the start of an element
			void StartElement
					(
					const XMLCh* const element_uri, 		// URI of element's namespace
 					const XMLCh* const element_local_name,	// local part of element's name
					const XMLCh* const element_qname,		// element's qname
					const Attributes& attr				// element's attributes
					);

			// process the end of an element
			void EndElement
					(
					const XMLCh* const element_uri, 		// URI of element's namespace
					const XMLCh* const element_local_name,	// local part of element's name
					const XMLCh* const element_qname		// element's qname
					);

		public:
			// ctor
			CParseHandlerWindowSpec
				(
				IMemoryPool *memory_pool,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// window specification generated by the parse handler
			CDXLWindowSpec *GetWindowKeyAt() const
			{
				return m_dxl_window_spec_gen;
			}
	};
}

#endif // !GPDXL_CParseHandlerWindowSpec_H

// EOF
