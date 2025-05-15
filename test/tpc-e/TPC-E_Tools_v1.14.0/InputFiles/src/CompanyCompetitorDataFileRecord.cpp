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

#include "../inc/CompanyCompetitorDataFileRecord.h"

#include <sstream>
#include <stdexcept>
#include <cstdlib> // for atoi, atof
// #include <string> // C++11 for stoi, stod

#include "../inc/Utilities.h"

using namespace TPCE;

CompanyCompetitorDataFileRecord::CompanyCompetitorDataFileRecord(const std::deque<std::string>& fields)
{
    if (fieldCount != fields.size())
    {
        throw std::runtime_error("Incorrect field count.");
    }

    cp_co_id = std::atoi(fields[0].c_str());
    //cp_co_id = std::stoi(fields[0]); // C++11
    cp_comp_co_id = std::atoi(fields[1].c_str());
    //cp_comp_co_id = std::stoi(fields[1]); // C++11

    DFRStringInit( fields[2], cp_in_id, cp_in_idCStr, maxCp_in_idLen);
}

TIdent CompanyCompetitorDataFileRecord::CP_CO_ID() const
{
    return cp_co_id;
}

TIdent CompanyCompetitorDataFileRecord::CP_COMP_CO_ID() const
{
    return cp_comp_co_id;
}

const std::string& CompanyCompetitorDataFileRecord::CP_IN_ID() const
{
    return cp_in_id;
}

const char* CompanyCompetitorDataFileRecord::CP_IN_ID_CSTR() const
{
    return cp_in_idCStr;
}

std::string CompanyCompetitorDataFileRecord::ToString(char fieldSeparator) const
{
    // Facilitate encapsulation by using public interface to fields.
    std::ostringstream msg;
    msg << CP_CO_ID() 
        << fieldSeparator << CP_COMP_CO_ID() 
        << fieldSeparator << CP_IN_ID();
    return msg.str();
}
