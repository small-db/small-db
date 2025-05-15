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
*   Class representing the Holdings, Trades, Trade Request, Settlement, Trade History, and Cash Transaction tables.
*/
#ifndef HOLDINGS_AND_TRADES_TABLE_H
#define HOLDINGS_AND_TRADES_TABLE_H

#include "EGenTables_stdafx.h"
#include "SecurityPriceRange.h"

namespace TPCE
{

// Arrays for min and max bounds on the security ranges for different tier accounts
// The indices into these arrays are
//      1) the customer tier (zero based)
//      2) the number of accounts for the customer (zero based)
// Entries with 0 mean there cannot be that many accounts for a customer with that tier.
//
const int iMinSecuritiesPerAccountRange[3][10] =
{{6, 4, 2, 2, 0, 0, 0, 0, 0, 0}
,{0, 7, 5, 4, 3, 2, 2, 2, 0, 0}
,{0, 0, 0, 0, 4, 4, 3, 3, 2, 2}};
const int iMaxSecuritiesPerAccountRange[3][10] =
{{14, 16, 18, 18, 00, 00, 00, 00, 00, 00}
,{00, 13, 15, 16, 17, 18, 18, 18, 00, 00}
,{00, 00, 00, 00, 16, 16, 17, 17, 18, 18}};
const int iMaxSecuritiesPerAccount = 18;    // maximum number of securities in a customer account

//const double fMinSecPrice = 20;
//const double fMaxSecPrice = 30;

// These are used for picking the transaction type at load time.
// NOTE that the corresponding "if" tests must be in the same order!
const int cMarketBuyLoadThreshold   = 30;                               //  1% - 30%
const int cMarketSellLoadThreshold  = cMarketBuyLoadThreshold   + 30;   // 31% - 60%
const int cLimitBuyLoadThreshold    = cMarketSellLoadThreshold  + 20;   // 61% - 80%
const int cLimitSellLoadThreshold   = cLimitBuyLoadThreshold    + 10;   // 81% - 90%
const int cStopLossLoadThreshold    = cLimitSellLoadThreshold   + 10;   // 91% - 100%

const int iPercentBuysOnMargin = 16;

// These are used when loading the table, and when generating runtime data.
const int   cNUM_TRADE_QTY_SIZES = 4;
const int   cTRADE_QTY_SIZES[cNUM_TRADE_QTY_SIZES] = {100, 200, 400, 800};

// Percentage of trades modifying holdings in Last-In-First-Out order.
//
const int   iPercentTradeIsLIFO = 35;

// Number of RNG calls for one simulated trade
const int iRNGSkipOneTrade = 11;    // average count for v3.5: 6.5

class CHoldingsAndTradesTable
{
    CRandom                     m_rnd;
    CCustomerAccountsAndPermissionsTable    m_CustomerAccountTable;

    TIdent                      m_iSecCount;    //number of securities
    UINT                        m_iMaxSecuritiesPerCA;  //number of securities per account
    TIdent                      m_SecurityIds[iMaxSecuritiesPerAccount];



public:
    //Constructor.
    CHoldingsAndTradesTable(CInputFiles inputFiles,
                            UINT        iLoadUnitSize,  // # of customers in one load unit
                            TIdent      iCustomerCount,
                            TIdent      iStartFromCustomer = iDefaultStartFromCustomer)
        : m_rnd(RNGSeedTableDefault)
        , m_CustomerAccountTable(inputFiles, iLoadUnitSize, iCustomerCount, iStartFromCustomer)
    {
        m_iSecCount = inputFiles.Securities->GetConfiguredSecurityCount();

        //Set the max number of holdings per account to be iMaxSecuritiesPerAccount
        //
        m_iMaxSecuritiesPerCA = iMaxSecuritiesPerAccount;
    };

    /*
    *   Reset the state for the next load unit.
    *   Called only from the loader (CTradeGen), not the driver.
    */
    void InitNextLoadUnit(INT64 TradesToSkip)
    {
        m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault,
                                        // there is only 1 call to this RNG per trade
                                          TradesToSkip));
    }

    /*
    *   Generate the number of securities for a given customer account.
    */
    int GetNumberOfSecurities(TIdent iCA_ID, eCustomerTier iTier, int iAccountCount)
    {
        RNGSEED OldSeed;
        int     iNumberOfSecurities;
        int     iMinRange, iMaxRange;   // for convenience

        iMinRange = iMinSecuritiesPerAccountRange[iTier - eCustomerTierOne][iAccountCount - 1];
        iMaxRange = iMaxSecuritiesPerAccountRange[iTier - eCustomerTierOne][iAccountCount - 1];

        OldSeed = m_rnd.GetSeed();
        m_rnd.SetSeed( m_rnd.RndNthElement( RNGSeedBaseNumberOfSecurities, iCA_ID ));
        iNumberOfSecurities = m_rnd.RndIntRange(iMinRange, iMaxRange);
        m_rnd.SetSeed( OldSeed );
        return( iNumberOfSecurities );
    }

    /*
    *   Get seed for the starting account id for a given customer id.
    *   This is needed for the driver to know what account ids belong to a given customer
    */
    RNGSEED GetStartingSecIDSeed(TIdent iCA_ID)
    {
        return( m_rnd.RndNthElement( RNGSeedBaseStartingSecurityID, iCA_ID * m_iMaxSecuritiesPerCA ));
    }

    /*
    *   Convert security index within an account (1-18) into
    *   corresponding security index within the
    *   Security.txt input file (0-6849).
    *
    *   Needed to be able to get the security symbol
    *   and other information from the input file.
    *
    *   RETURNS:
    *           security index within the input file (0-based)
    */
    TIdent GetSecurityFlatFileIndex(
            TIdent  iCustomerAccount,
            UINT    iSecurityAccountIndex)
    {
        RNGSEED OldSeed;
        TIdent  iSecurityFlatFileIndex; // index of the selected security in the input flat file
        UINT    iGeneratedIndexCount = 0;   // number of currently generated unique flat file indexes
        UINT    i;

        OldSeed = m_rnd.GetSeed();
        m_rnd.SetSeed( GetStartingSecIDSeed( iCustomerAccount ));

        iGeneratedIndexCount = 0;

        while (iGeneratedIndexCount < iSecurityAccountIndex)
        {
            iSecurityFlatFileIndex = m_rnd.RndInt64Range(0, m_iSecCount-1);

            for (i = 0; i < iGeneratedIndexCount; ++i)
            {
                if (m_SecurityIds[i] == iSecurityFlatFileIndex)
                    break;
            }

            // If a duplicate is found, overwrite it in the same location
            // so basically no changes are made.
            //
            m_SecurityIds[i] = iSecurityFlatFileIndex;

            // If no duplicate is found, increment the count of unique ids
            //
            if (i == iGeneratedIndexCount)
            {
                ++iGeneratedIndexCount;
            }
        }

        m_rnd.SetSeed( OldSeed );

        return iSecurityFlatFileIndex;
    }

    /*
    *   Generate random customer account and security to perfrom a trade on.
    *   This function is used by both the runtime driver (CCETxnInputGenerator) and
    *   by the loader when generating initial trades (CTradeGen).
    *
    */
    void GenerateRandomAccountSecurity(
            TIdent          iCustomer,                  // in
            eCustomerTier   iTier,                      // in
            TIdent*         piCustomerAccount,          // out
            TIdent*         piSecurityFlatFileIndex,    // out
            int*            piSecurityAccountIndex)     // out
    {
        TIdent  iCustomerAccount;
        int     iAccountCount;
        int     iTotalAccountSecurities;
        int     iSecurityAccountIndex;  // index of the selected security in the account's basket
        TIdent  iSecurityFlatFileIndex; // index of the selected security in the input flat file

        // Select random account for the customer
        //
        m_CustomerAccountTable.GenerateRandomAccountId( m_rnd, iCustomer, iTier,
                                                        &iCustomerAccount, &iAccountCount);

        iTotalAccountSecurities = GetNumberOfSecurities(iCustomerAccount, iTier, iAccountCount);

        // Select random security in the account
        //
        iSecurityAccountIndex = m_rnd.RndIntRange(1, iTotalAccountSecurities);

        iSecurityFlatFileIndex = GetSecurityFlatFileIndex(iCustomerAccount, iSecurityAccountIndex);

        // Return data
        //
        *piCustomerAccount          = iCustomerAccount;
        *piSecurityFlatFileIndex    = iSecurityFlatFileIndex;
        if (piSecurityAccountIndex != NULL)
        {
            *piSecurityAccountIndex = iSecurityAccountIndex;
        }
    }

    bool IsAbortedTrade(TIdent TradeId)
    {
        bool bResult = false;
            if( iAbortedTradeModFactor == TradeId % iAbortTrade )
        {
            bResult = true;
        }
        return bResult;
    }
};

}   // namespace TPCE

#endif //HOLDINGS_AND_TRADES_TABLE_H
