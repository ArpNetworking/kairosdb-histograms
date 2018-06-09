KairosDB Histograms
===================

Histograms are a method of storing multiple samples in a compact way that allows for 
reaggregation of most statistics (with a loss of precision).  This plugin creates a histogram 
datapoint type and recreates many of the aggregators that are used in stock KairosDB.  In
addition, it adds 2 new aggregators: merge and apdex.

Merge
-----


Apdex
