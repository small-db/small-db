#ifndef TAX_RATE_DIVISION_DATA_FILE_RECORD_H
#define TAX_RATE_DIVISION_DATA_FILE_RECORD_H

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
* - Doug Johnson
*/

#include <deque>
#include <string>

#include "ITaxRateFileRecord.h"

namespace TPCE
{
    //
    // Description:
    // A class to represent a single record in the TaxRateDivision data file.
    //
    // Exception Safety:
    // The Basic guarantee is provided.
    //
    // Copy Behavior:
    // Copying is allowed.
    //

    class TaxRateDivisionDataFileRecord : public ITaxRateFileRecord
    {
    private:
        static const int maxTx_idLen = 4;
        char tx_idCStr[maxTx_idLen+1];
        std::string tx_id;

        static const int maxTx_nameLen = 50;
        char tx_nameCStr[maxTx_nameLen+1];
        std::string tx_name;

        double tx_rate;

        static const unsigned int fieldCount = 3;

    public:
        explicit TaxRateDivisionDataFileRecord(const std::deque<std::string>& fields);

        //
        // Default copies and destructor are ok.
        //
        // ~TaxRateDivisionDataFileRecord()
        // TaxRateDivisionDataFileRecord(const TaxRateDivisionDataFileRecord&);
        // TaxRateDivisionDataFileRecord& operator=(const TaxRateDivisionDataFileRecord&);
        //

        const std::string& TX_ID() const;
        const char* TX_ID_CSTR() const;

        const std::string& TX_NAME() const;
        const char* TX_NAME_CSTR() const;

        double TX_RATE() const;

        std::string ToString(char fieldSeparator = '\t') const;
    };

}   // namespace TPCE
#endif // TAX_RATE_DIVISION_DATA_FILE_RECORD_H
