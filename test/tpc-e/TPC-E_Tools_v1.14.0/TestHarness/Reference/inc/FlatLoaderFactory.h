/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
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
*   Flat file loader class factory.
*   This class instantiates particular table loader classes.
*/
#ifndef FLAT_LOADER_FACTORY_H
#define FLAT_LOADER_FACTORY_H

namespace TPCE
{

class CFlatLoaderFactory : public CBaseLoaderFactory
{
    char                    m_szOutDir[iMaxPath];
    FlatFileOutputModes     m_eOutputMode;  // overwrite/append

public:
    // Constructor
    CFlatLoaderFactory(char *szOutDir, FlatFileOutputModes eOutputMode)
        : m_eOutputMode(eOutputMode)
    {
        assert(szOutDir);

        strncpy(m_szOutDir, szOutDir, sizeof(m_szOutDir));
    };

    // Functions to create loader classes for individual tables.

    virtual CBaseLoader<ACCOUNT_PERMISSION_ROW>*    CreateAccountPermissionLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "AccountPermission.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatAccountPermissionLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<ADDRESS_ROW>*               CreateAddressLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Address.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatAddressLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<BROKER_ROW>*                CreateBrokerLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Broker.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatBrokerLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<CASH_TRANSACTION_ROW>*      CreateCashTransactionLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "CashTransaction.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatCashTransactionLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<CHARGE_ROW>*                CreateChargeLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Charge.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatChargeLoad(szFileName, m_eOutputMode);
    };

    virtual CBaseLoader<COMMISSION_RATE_ROW>*       CreateCommissionRateLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "CommissionRate.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatCommissionRateLoad(szFileName, m_eOutputMode);
    };

    virtual CBaseLoader<COMPANY_COMPETITOR_ROW>*    CreateCompanyCompetitorLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "CompanyCompetitor.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatCompanyCompetitorLoad(szFileName, m_eOutputMode);
    };

    virtual CBaseLoader<COMPANY_ROW>*               CreateCompanyLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Company.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatCompanyLoad(szFileName, m_eOutputMode);
    };

    virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW>*      CreateCustomerAccountLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "CustomerAccount.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatCustomerAccountLoad(szFileName, m_eOutputMode);
    };

    virtual CBaseLoader<CUSTOMER_ROW>*              CreateCustomerLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Customer.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatCustomerLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<CUSTOMER_TAXRATE_ROW>*      CreateCustomerTaxrateLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "CustomerTaxrate.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatCustomerTaxrateLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<DAILY_MARKET_ROW>*          CreateDailyMarketLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "DailyMarket.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatDailyMarketLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<EXCHANGE_ROW>*              CreateExchangeLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Exchange.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatExchangeLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<FINANCIAL_ROW>*             CreateFinancialLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Financial.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatFinancialLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<HOLDING_ROW>*               CreateHoldingLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Holding.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatHoldingLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<HOLDING_HISTORY_ROW>*       CreateHoldingHistoryLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "HoldingHistory.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatHoldingHistoryLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<HOLDING_SUMMARY_ROW>*           CreateHoldingSummaryLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "HoldingSummary.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatHoldingSummaryLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<INDUSTRY_ROW>*              CreateIndustryLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Industry.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatIndustryLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<LAST_TRADE_ROW>*            CreateLastTradeLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "LastTrade.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatLastTradeLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<NEWS_ITEM_ROW>*             CreateNewsItemLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "NewsItem.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatNewsItemLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<NEWS_XREF_ROW>*             CreateNewsXRefLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "NewsXRef.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatNewsXRefLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<SECTOR_ROW>*                CreateSectorLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Sector.txt", sizeof(szFileName) - strlen(m_szOutDir) - 11 );

        return new CFlatSectorLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<SECURITY_ROW>*              CreateSecurityLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Security.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatSecurityLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<SETTLEMENT_ROW>*            CreateSettlementLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Settlement.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatSettlementLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<STATUS_TYPE_ROW>*           CreateStatusTypeLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "StatusType.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatStatusTypeLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<TAXRATE_ROW>*               CreateTaxrateLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Taxrate.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatTaxrateLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<TRADE_HISTORY_ROW>*         CreateTradeHistoryLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "TradeHistory.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatTradeHistoryLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<TRADE_ROW>*                 CreateTradeLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "Trade.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatTradeLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<TRADE_REQUEST_ROW>*         CreateTradeRequestLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "TradeRequest.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatTradeRequestLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<TRADE_TYPE_ROW>*            CreateTradeTypeLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "TradeType.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatTradeTypeLoad(szFileName, m_eOutputMode);
    };
    virtual CBaseLoader<WATCH_ITEM_ROW>*            CreateWatchItemLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "WatchItem.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatWatchItemLoad(szFileName, m_eOutputMode);
    };

    virtual CBaseLoader<WATCH_LIST_ROW>*            CreateWatchListLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "WatchList.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatWatchListLoad(szFileName, m_eOutputMode);
    };

    virtual CBaseLoader<ZIP_CODE_ROW>*          CreateZipCodeLoader()
    {
        char    szFileName[iMaxPath];

        // snprintf doesn't exist on every platform, so do two string
        // manipulations if we want to check the buffer length
        strncpy( szFileName, m_szOutDir, sizeof(szFileName)-1 );
        strncat( szFileName, "ZipCode.txt", sizeof(szFileName) - strlen(m_szOutDir) - 1 );

        return new CFlatZipCodeLoad(szFileName, m_eOutputMode);
    };
};

}   // namespace TPCE

#endif //FLAT_LOADER_FACTORY_H
