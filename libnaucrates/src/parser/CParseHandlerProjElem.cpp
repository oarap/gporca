//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerProjElem.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing proj elem operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerProjElem.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProjElem::CParseHandlerProjElem
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerProjElem::CParseHandlerProjElem(IMemoryPool *memory_pool,
											 CParseHandlerManager *parse_handler_mgr,
											 CParseHandlerBase *parse_handler_root)
	: CParseHandlerScalarOp(memory_pool, parse_handler_mgr, parse_handler_root)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProjElem::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProjElem::StartElement(const XMLCh *const,  // element_uri,
									const XMLCh *const element_local_name,
									const XMLCh *const,  // element_qname
									const Attributes &attrs)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarProjElem),
									  element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, str->GetBuffer());
	}

	// parse and create proj elem operator
	m_dxl_op = (CDXLScalarProjElem *) CDXLOperatorFactory::MakeDXLProjElem(
		m_parse_handler_mgr->GetDXLMemoryManager(), attrs);

	// create and activate the parse handler for the child scalar expression node

	CParseHandlerBase *parse_handler_root = CParseHandlerFactory::GetParseHandler(
		m_memory_pool, CDXLTokens::XmlstrToken(EdxltokenScalar), m_parse_handler_mgr, this);
	m_parse_handler_mgr->ActivateParseHandler(parse_handler_root);

	// store parse handler
	this->Append(parse_handler_root);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerProjElem::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerProjElem::EndElement(const XMLCh *const,  // element_uri,
								  const XMLCh *const element_local_name,
								  const XMLCh *const  // element_qname
)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarProjElem),
									  element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(
			m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, str->GetBuffer());
	}

	// construct node from the parsed expression node
	m_dxl_node = GPOS_NEW(m_memory_pool) CDXLNode(m_memory_pool, m_dxl_op);

	CParseHandlerScalarOp *operator_parse_handler =
		dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);

	// store constructed child
	AddChildFromParseHandler(operator_parse_handler);

	// deactivate handler
	m_parse_handler_mgr->DeactivateHandler();
}

// EOF
