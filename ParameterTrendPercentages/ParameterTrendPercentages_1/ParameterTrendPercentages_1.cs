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
    private static GQIStringColumn _severityColumn = new GQIStringColumn("Value");
    private static GQIDoubleColumn _percentageColumn = new GQIDoubleColumn("Percentage");

    private static GQIStringArgument _paramIdArg = new GQIStringArgument("Parameter Id") { IsRequired = true };
    private static GQIDateTimeArgument _startArg = new GQIDateTimeArgument("Start") { IsRequired = false };
    private static GQIDateTimeArgument _endArg = new GQIDateTimeArgument("End") { IsRequired = false };

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
        var paramID = args.GetArgumentValue(_paramIdArg);
        var result = new OnArgumentsProcessedOutputArgs();
        if (string.IsNullOrWhiteSpace(paramID))
            return result;

        var parts = paramID.Split('/');
        if (parts.Length != 3)
            return result;

        if (int.TryParse(parts[0], out var dmaID) &&
            int.TryParse(parts[1], out var elementID) &&
            int.TryParse(parts[2], out var parameterID))
            _parameter = new ParameterID(dmaID, elementID, parameterID);

        args.TryGetArgumentValue(_startArg, out _start);
        args.TryGetArgumentValue(_endArg, out _end);

        return result;
    }

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
            _severityColumn,
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

    private static string DMADateTimeToStringFormat(DateTime input)
    {
        return input.ToString("yyyy'-'MM'-'dd HH':'mm':'ss", CultureInfo.InvariantCulture);
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

        if (_start == _end)
            throw new DataMinerException("Timeslot cannot have same start and end time - skipping check.");

        var getTrendDataMessage = new GetTrendDataMessage
        {
            DataMinerID = _parameter.DataMinerID,
            ElementID = _parameter.ElementID,
            Parameters = new[] { new ParameterIndexPair(_parameter.ParameterID_) },
            StartTime = _start,
            EndTime = _end,
            ReturnAsObjects = true,
            TrendingType = trendingType,
        };

        var getTrendDataResponseMessage = (GetTrendDataResponseMessage)_callback.SendMessage(getTrendDataMessage);
        List<TrendRecord> records = getTrendDataResponseMessage.Records.Values.FirstOrDefault().OrderBy(r => r.Time).ToList() ?? throw new DataMinerException("Failed to retrieve trend information for the provided timeslot");

        // Search all records before start time and add closest with updated start time.
        Dictionary<DateTime, string> trendRecords = new Dictionary<DateTime, string>();
        List<TrendRecord> beforeStartRecords = records.Where(r => r.Time <= _start).ToList();
        if (beforeStartRecords.Count > 0)
            trendRecords.Add(_start, beforeStartRecords.Last().GetStringValue());

        // Add trend records to Dictionary if value is not empty and within window
        foreach (var record in records.Where(r => r.GetStringValue() != string.Empty && r.Time > _start && r.Time < _end))
        {
            if (!trendRecords.ContainsKey(record.Time))
                trendRecords.Add(record.Time, record.GetStringValue());
        }

        // Count the different number of string values
        _result = CalculateStringPercentages(trendRecords);
    }

    private Dictionary<string, double> CalculateStringPercentages(Dictionary<DateTime, string> dictionary)
    {
        int totalCount = dictionary.Count;

        var stringPercentages = dictionary
            .GroupBy(entry => entry.Value)
            .Select(group => new { StringValue = group.Key, Percentage = (double)group.Count() / totalCount * 100 })
            .ToDictionary(item => item.StringValue, item => Math.Round(item.Percentage, 2));

        return stringPercentages;
    }
}