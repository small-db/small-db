/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
 */

/*
*   Database loader class for TRADE_REQUEST table.
*/
#ifndef ODBC_TRADE_REQUEST_LOAD_H
#define ODBC_TRADE_REQUEST_LOAD_H

namespace TPCE
{

class CODBCTradeRequestLoad : public CDBLoader <TRADE_REQUEST_ROW>
{

public:
    CODBCTradeRequestLoad(char *szServer, char *szDatabase, char *szLoaderParams, char *szTable = "TRADE_REQUEST")
        : CDBLoader<TRADE_REQUEST_ROW>(szServer, szDatabase, szLoaderParams, szTable)
    {
    };

    virtual void BindColumns()
    {
        //Binding function we have to implement.
        int i = 0;
        if (   bcp_bind(m_hdbc, (BYTE *) &m_row.TR_T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
            || bcp_bind(m_hdbc, (BYTE *) &m_row.TR_TT_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
            || bcp_bind(m_hdbc, (BYTE *) &m_row.TR_S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
            || bcp_bind(m_hdbc, (BYTE *) &m_row.TR_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
            || bcp_bind(m_hdbc, (BYTE *) &m_row.TR_BID_PRICE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
            || bcp_bind(m_hdbc, (BYTE *) &m_row.TR_B_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
            )
            ThrowError(CODBCERR::eBcpBind);

        if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (TR_T_ID)" ) != SUCCEED )
            ThrowError(CODBCERR::eBcpControl);
    };

};

}   // namespace TPCE

#endif //ODBC_TRADE_REQUEST_LOAD_H
