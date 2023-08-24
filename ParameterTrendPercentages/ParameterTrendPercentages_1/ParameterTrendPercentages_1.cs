/*
****************************************************************************
*  Copyright (c) 2023,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

11/08/2023	1.0.0.1		LDR, Skyline	Initial version
****************************************************************************
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Exceptions;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Messages.SLDataGateway;
using Skyline.DataMiner.Net.Trending;

[GQIMetaData(Name = "Parameter trend percentages")]
public class CSVDataSource : IGQIOnInit, IGQIDataSource, IGQIInputArguments, IGQIOnPrepareFetch
{
    private static readonly GQIStringColumn _keyColumn = new GQIStringColumn("Key");
    private static readonly GQIDoubleColumn _percentageColumn = new GQIDoubleColumn("Percentage");

    private static readonly GQIStringArgument _paramIdArg = new GQIStringArgument("Parameter Id") { IsRequired = true };
    private static readonly GQIDateTimeArgument _startArg = new GQIDateTimeArgument("Start") { IsRequired = false };
    private static readonly GQIDateTimeArgument _endArg = new GQIDateTimeArgument("End") { IsRequired = false };

    private ParameterID _parameter;
    private DateTime _start;
    private DateTime _end;
    private GQIDMS _callback;
    private Dictionary<string, double> _result;

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[]
        {
            _paramIdArg,
            _startArg,
            _endArg,
        };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        var result = new OnArgumentsProcessedOutputArgs();

        // Process parameter information
        var paramID = args.GetArgumentValue(_paramIdArg);
        if (string.IsNullOrWhiteSpace(paramID))
            return result;

        var parts = paramID.Split('/');
        if (parts.Length == 3 || parts.Length == 4)
        {
            if (int.TryParse(parts[0], out var dmaID) &&
                int.TryParse(parts[1], out var elementID) &&
                int.TryParse(parts[2], out var parameterID))
            {
                _parameter = parts.Length == 3
                    ? new ParameterID(dmaID, elementID, parameterID)
                    : new ParameterID(dmaID, elementID, parameterID, parts[3]);
            }
        }
        else
        {
            return result;
        }

        args.TryGetArgumentValue(_startArg, out _start);
        args.TryGetArgumentValue(_endArg, out _end);

        return result;
    }

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
            _keyColumn,
            _percentageColumn,
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        List<GQIRow> rows = new List<GQIRow>();

        foreach (var item in _result)
        {
            GQIRow row = FormatRow(item.Key, item.Value);
            rows.Add(row);
        }

        return new GQIPage(rows.ToArray())
        {
            HasNextPage = false,
        };
    }

    public OnPrepareFetchOutputArgs OnPrepareFetch(OnPrepareFetchInputArgs args)
    {
        if (_parameter == null)
            throw new GenIfException("Invalid parameter ID.");

        DateTime start;
        DateTime end;
        if (_start != DateTime.MinValue && _end != DateTime.MinValue)
        {
            start = _start;
            end = _end;
        }
        else
        {
            var now = DateTime.UtcNow;
            start = DateTime.UtcNow - TimeSpan.FromHours(24);
            end = now;
        }

        RetrieveTrendRecords();

        return new OnPrepareFetchOutputArgs();
    }

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _callback = args.DMS;
        return new OnInitOutputArgs();
    }

    private static GQIRow FormatRow(string value, double percentage)
    {
        return new GQIRow(new[]
        {
            new GQICell() {Value = value },
            FormatPercentageCell(percentage),
        });
    }

    private static GQICell FormatPercentageCell(double percentage)
    {
        return new GQICell() { Value = percentage, DisplayValue = $"{Math.Round(percentage, 2)} %" };
    }

    /// <summary>
    /// Retrieve Trend records.
    /// </summary>
    /// <exception cref="DataMinerException">If no records could be found.</exception>
    private void RetrieveTrendRecords()
    {
        TrendingType trendingType = TrendingType.Realtime;

        if (_start >= _end)
            throw new DataMinerException("Invalid time provided.");

        var getTrendDataMessage = new GetTrendDataMessage
        {
            DataMinerID = _parameter.DataMinerID,
            ElementID = _parameter.ElementID,
            Parameters = _parameter.Instance == null ? new[] { new ParameterIndexPair(_parameter.ParameterID_) } : new[] { new ParameterIndexPair(_parameter.ParameterID_, _parameter.Instance) },
            StartTime = _start,
            EndTime = _end,
            ReturnAsObjects = true,
            TrendingType = trendingType,
        };

        var getTrendDataResponseMessage = (GetTrendDataResponseMessage)_callback.SendMessage(getTrendDataMessage);
        List<TrendRecord> records = getTrendDataResponseMessage.Records.Values.FirstOrDefault().OrderBy(r => r.Time).ToList() ?? throw new DataMinerException("Failed to retrieve trend information for the provided timeslot");

        // Remove empty values or outside window of request
        List<TrendRecord> filteredRecords = records
            .Where(r => r.Time >= _start && r.Time <= _end && !string.IsNullOrEmpty(r.GetStringValue()))
            .ToList();

        // Convert the filtered records into a dictionary
        Dictionary<DateTime, string> trendRecords = filteredRecords
            .ToDictionary(r => r.Time, r => r.GetStringValue());

        // Count the different number of string values
        _result = CalculateKeyDistribution(trendRecords);
    }

    private Dictionary<string, double> CalculateKeyDistribution(Dictionary<DateTime, string> dictionary)
    {
        int totalCount = dictionary.Count;

        var distribution = dictionary
            .GroupBy(entry => entry.Value)
            .ToDictionary(
                group => group.Key,
                group => Math.Round((double)group.Count() / totalCount * 100, 2));

        return distribution;
    }
}