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
    private static readonly GQIDoubleColumn _absoluteColumn = new GQIDoubleColumn("Absolute");

    private static readonly GQIStringArgument _paramIdArg = new GQIStringArgument("Parameter Id") { IsRequired = true };
    private static readonly GQIDateTimeArgument _startArg = new GQIDateTimeArgument("Start") { IsRequired = false };
    private static readonly GQIDateTimeArgument _endArg = new GQIDateTimeArgument("End") { IsRequired = false };

    private ParameterID _parameter;
    private DateTime _start;
    private DateTime _end;
    private List<ResultRow> _result;

    private GQIDMS _callback;

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

        _start = args.GetArgumentValue(_startArg);
        _end = args.GetArgumentValue(_endArg);

        return result;
    }

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
            _keyColumn,
            _percentageColumn,
            _absoluteColumn,
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        List<GQIRow> rows = new List<GQIRow>();

        foreach (var item in _result)
        {
            GQIRow row = FormatRow(item.Key, item.Percentage, item.Absolute);
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

        _start = TimeZoneInfo.ConvertTimeFromUtc(_start, TimeZoneInfo.Local);
        _end = TimeZoneInfo.ConvertTimeFromUtc(_end, TimeZoneInfo.Local);

        RetrieveTrendRecords();

        return new OnPrepareFetchOutputArgs();
    }

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _callback = args.DMS;
        return new OnInitOutputArgs();
    }

    private static GQIRow FormatRow(string value, double percentage, double absolute)
    {
        return new GQIRow(new[]
        {
            new GQICell() {Value = value },
            FormatPercentageCell(percentage),
            FormatAbsoluteCell(absolute),
        });
    }

    private static GQICell FormatPercentageCell(double percentage)
    {
        return new GQICell() { Value = percentage, DisplayValue = $"{Math.Round(percentage, 2)} %" };
    }

    private static GQICell FormatAbsoluteCell(double absolute)
    {
        return new GQICell() { Value = absolute, DisplayValue = $"{absolute}" };
    }

    /// <summary>
    /// Retrieve Trend records.
    /// </summary>
    /// <exception cref="DataMinerException">If no records could be found.</exception>
    private void RetrieveTrendRecords()
    {
        TrendingType trendingType = TrendingType.Realtime;

        // Throw exception if time range is incorrect.
        if (_start >= _end)
            throw new DataMinerException("Invalid time provided.");

        // Retrieve trending information
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
        var response = getTrendDataResponseMessage;

        // Process trending information
        Dictionary<string, double> result = new Dictionary<string, double>();
        if (response.Records.Values.FirstOrDefault() != null)
        {
            List<TrendRecord> records = response.Records.Values.FirstOrDefault().OrderBy(r => r.Time).ToList();
            Dictionary<DateTime, string> trendRecords = new Dictionary<DateTime, string>();

            // Search all records before start time and add closest with updated start time
            List<TrendRecord> beforeStartRecords = records.Where(r => r.Time <= _start).ToList();
            trendRecords.Add(_start, beforeStartRecords.Count > 0 ? beforeStartRecords.Last().GetStringValue() : "Not trended");

            // Add trend records to Dictionary if value is not empty and within window
            foreach (var record in records.Where(r => r.GetStringValue() != string.Empty && r.Time > _start && r.Time < _end))
            {
                // Remove milliseconds to avoid duplicate time records
                record.Time.AddMilliseconds(record.Time.Millisecond * -1);

                if (!trendRecords.ContainsKey(record.Time))
                    trendRecords.Add(record.Time, record.GetStringValue());
            }

            // Stretch last known value to end time
            if (trendRecords.Last().Key != _end)
                trendRecords.Add(_end, trendRecords.Last().Value);

            // Calculate percentages
            _result = Calculate(trendRecords);
        }
        else
        {
            // If no points are returned, indicate that no trending is enabled.
            _result = new List<ResultRow>
            {
                new ResultRow() { Key = "Not trended", Percentage = 100, Absolute = 0 },
            };
        }
    }

    /// <summary>
    /// Calculate Percentages.
    /// </summary>
    /// <param name="records">Trend records dictionary.</param>
    /// <returns>Dictionary with each distinct value as key and the percentage as value.</returns>
    private List<ResultRow> Calculate(Dictionary<DateTime, string> records)
    {
        int totalDuration = 0;
        var sortedRecords = records.OrderBy(entry => entry.Key).ToList();
        Dictionary<string, int> activeTimes = new Dictionary<string, int>();

        for (int i = 0; i < sortedRecords.Count - 1; i++)
        {
            // Calculate duration
            int duration = (int)(sortedRecords[i + 1].Key - sortedRecords[i].Key).TotalSeconds;

            // Add key if does not yet exist
            if (!activeTimes.ContainsKey(sortedRecords[i].Value))
                activeTimes.Add(sortedRecords[i].Value, duration);
            else
                activeTimes[sortedRecords[i].Value] += duration;

            // Calculate total duration for percentage calculation.
            totalDuration += duration;
        }

        // Calculate and return
        return activeTimes
        .Select(kv => new ResultRow
        {
            Key = kv.Key,
            Percentage = ((double)kv.Value / totalDuration) * 100,
            Absolute = kv.Value,
        })
        .ToList();
    }
}

/// <summary>
/// ResultRow class.
/// </summary>
public class ResultRow
{
    public string Key { get; set; }

    public double Percentage { get; set; }

    public double Absolute { get; set; }
}