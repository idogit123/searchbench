-- SearchBench 92 queries in RQL, one per line (the driver is line-based).
-- See README.md for the reasoning behind each choice below.
--
-- Counts and joins end `limit 0, 0`: ./query reports TotalResults, not rows.
-- Joins take their distinct count from `select distinct TraceId`, with the
-- HasPayment/HasFrontend/HasCart are fields on each log, written at load
-- time from a single pass over the corpus (see trace_flags.py).
--
-- Aggregations use select facet(): RQL's `group by` builds an auto map-reduce
-- index and is unavailable over a static index. $top20 is declared by ./query.
--
-- Wildcards: trailing goes through search(Body, 'charg*'); leading and mid use
-- endsWith() or regex(), because the analyzer strips '*' and '?'.
--
-- Two indexes. Logs/Search is Corax and serves 84 queries. Logs/Fuzzy is Lucene
-- and serves Q13, Q22-Q24, Q48, Q49 and Q59, which need the fuzzy and proximity
-- Corax does not support. Q53 is there too: on 7.2.6 the Corax path for a range
-- combined with order by score() is materially slower.

-- Q01 task=count filter=term freq=hi
from index 'Logs/Search' where search(Body, 'error') limit 0, 0
-- Q02 task=count filter=term freq=lo
from index 'Logs/Search' where search(Body, 'payment') limit 0, 0
-- Q03 task=count filter=and freq=hi (AND, 2 tokens)
from index 'Logs/Search' where search(Body, 'failed order', and) limit 0, 0
-- Q04 task=count filter=and freq=hi (AND, 4 tokens)
from index 'Logs/Search' where search(Body, 'failed charge card cache', and) limit 0, 0
-- Q05 task=count filter=and freq=lo (AND, 8 tokens)
from index 'Logs/Search' where search(Body, 'failed send order confirmation email service expected post', and) limit 0, 0
-- Q06 task=count filter=or freq=hi (OR, 2 tokens)
from index 'Logs/Search' where search(Body, 'error failed') limit 0, 0
-- Q07 task=count filter=or freq=hi (OR, 4 tokens)
from index 'Logs/Search' where search(Body, 'connection request conversion post') limit 0, 0
-- Q08 task=count filter=or freq=mid (OR, 8 tokens)
from index 'Logs/Search' where search(Body, 'payment exception refused send confirmation email expected deadline') limit 0, 0
-- Q09 task=count filter=or,minmatch freq=hi (>=2 of 4)
from index 'Logs/Search' where (search(Body, 'error failed', and) or search(Body, 'error charge', and) or search(Body, 'error cache', and) or search(Body, 'failed charge', and) or search(Body, 'failed cache', and) or search(Body, 'charge cache', and)) limit 0, 0
-- Q10 task=count filter=phrase freq=mid (phrase, 2 tokens)
from index 'Logs/Search' where search(Body, '"place order"') limit 0, 0
-- Q11 task=count filter=phrase freq=mid (phrase, 4 tokens)
from index 'Logs/Search' where search(Body, '"failed to place order"') limit 0, 0
-- Q12 task=count filter=phrase freq=lo (phrase, 8 tokens)
from index 'Logs/Search' where search(Body, '"post to email service expected 200 got 500"') limit 0, 0
-- Q13 task=count filter=phrase,proximity freq=hi (failed within 2 of order)
from index 'Logs/Fuzzy' where proximity(search(Body, '"failed order"'), 2) limit 0, 0
-- Q14 task=count filter=phrase,or freq=hi (phrase OR term)
from index 'Logs/Search' where search(Body, '"failed to place order"') or search(Body, 'charge') limit 0, 0
-- Q15 task=count filter=phrase,and freq=mid (phrase AND term)
from index 'Logs/Search' where search(Body, '"failed to place order"') and search(Body, 'charge') limit 0, 0
-- Q16 task=count filter=regexp freq=hi (charg.*)
from index 'Logs/Search' where search(Body, 'charg*') limit 0, 0
-- Q17 task=count filter=regexp freq=hi (ord.*)
from index 'Logs/Search' where search(Body, 'ord*') limit 0, 0
-- Q18 task=count filter=regexp freq=mid (conn.*)
from index 'Logs/Search' where search(Body, 'conn*') limit 0, 0
-- Q19 task=count filter=regexp freq=hi (single-char wildcard mid: c.che -> cache)
from index 'Logs/Search' where regex(Body, '^c.che$') limit 0, 0
-- Q20 task=count filter=prefix freq=mid (conn)
from index 'Logs/Search' where search(Body, 'conn*') limit 0, 0
-- Q21 task=count filter=prefix freq=hi (charg)
from index 'Logs/Search' where search(Body, 'charg*') limit 0, 0
-- Q22 task=count filter=fuzzy freq=mid (levenshtein distance 1)
from index 'Logs/Fuzzy' where fuzzy(Body = 'connection', 0.89) limit 0, 0
-- Q23 task=count filter=fuzzy freq=mid (levenshtein distance 2)
from index 'Logs/Fuzzy' where fuzzy(Body = 'connection', 0.79) limit 0, 0
-- Q24 task=count filter=fuzzy,prefix freq=mid (levenshtein-2 AND prefix, same 'conn' root)
from index 'Logs/Fuzzy' where fuzzy(Body = 'connection', 0.79) and search(Body, 'conn*') limit 0, 0
-- Q25 task=count filter=like freq=mid (prefix wildcard conn%)
from index 'Logs/Search' where search(Body, 'conn*') limit 0, 0
-- Q26 task=count filter=like freq=hi (suffix wildcard %tion)
from index 'Logs/Search' where endsWith(Body, 'tion') limit 0, 0
-- Q27 task=count filter=like freq=mid (middle wildcard %nnec%)
from index 'Logs/Search' where regex(Body, 'nnec') limit 0, 0
-- Q28 task=count filter=and,negation freq=hi (error but NOT cache)
from index 'Logs/Search' where search(Body, 'error') and not search(Body, 'cache') limit 0, 0
-- Q29 task=count filter=or,negation freq=hi (error/failed, excluding charge)
from index 'Logs/Search' where search(Body, 'error failed') and not search(Body, 'charge') limit 0, 0
-- Q30 task=count filter=term,window freq=hi (term + Timestamp BETWEEN 6h)
from index 'Logs/Search' where search(Body, 'error') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' limit 0, 0
-- Q31 task=count filter=and,window freq=hi (term + service + Timestamp BETWEEN 6h)
from index 'Logs/Search' where ServiceName = 'frontend' and search(Body, 'failed') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' limit 0, 0
-- Q32 task=count filter=or,window freq=mid (8-token OR within a BETWEEN 6h window)
from index 'Logs/Search' where search(Body, 'payment exception refused send confirmation email expected deadline') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' limit 0, 0

-- Q33 task=top_k filter=term freq=hi
from index 'Logs/Search' where search(Body, 'charge') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q34 task=top_k filter=term freq=mid
from index 'Logs/Search' where search(Body, 'connection') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q35 task=top_k filter=term freq=lo
from index 'Logs/Search' where search(Body, 'payment') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q36 task=top_k filter=and freq=hi (AND, 2 tokens)
from index 'Logs/Search' where search(Body, 'failed order', and) order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q37 task=top_k filter=and freq=hi (AND, 4 tokens)
from index 'Logs/Search' where search(Body, 'failed charge card cache', and) order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q38 task=top_k filter=and freq=lo (AND, 8 tokens)
from index 'Logs/Search' where search(Body, 'failed send order confirmation email service expected post', and) order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q39 task=top_k filter=or freq=hi (OR, 2 tokens)
from index 'Logs/Search' where search(Body, 'error failed') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q40 task=top_k filter=or freq=hi (OR, 4 tokens)
from index 'Logs/Search' where search(Body, 'connection request conversion post') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q41 task=top_k filter=or freq=mid (OR, 8 tokens)
from index 'Logs/Search' where search(Body, 'payment exception refused send confirmation email expected deadline') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q42 task=top_k filter=or,minmatch freq=hi (>=2 of 4)
from index 'Logs/Search' where (search(Body, 'error failed', and) or search(Body, 'error charge', and) or search(Body, 'error cache', and) or search(Body, 'failed charge', and) or search(Body, 'failed cache', and) or search(Body, 'charge cache', and)) order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q43 task=top_k filter=phrase freq=mid (phrase, 2 tokens)
from index 'Logs/Search' where search(Body, '"place order"') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q44 task=top_k filter=phrase freq=mid (phrase, 4 tokens)
from index 'Logs/Search' where search(Body, '"failed to place order"') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q45 task=top_k filter=phrase freq=lo (phrase, 8 tokens)
from index 'Logs/Search' where search(Body, '"post to email service expected 200 got 500"') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q46 task=top_k filter=regexp freq=mid (conn.*)
from index 'Logs/Search' where search(Body, 'conn*') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q47 task=top_k filter=prefix freq=hi (charg)
from index 'Logs/Search' where search(Body, 'charg*') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q48 task=top_k filter=fuzzy freq=mid (levenshtein distance 1)
from index 'Logs/Fuzzy' where fuzzy(Body = 'connection', 0.89) order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q49 task=top_k filter=fuzzy freq=mid (levenshtein distance 2)
from index 'Logs/Fuzzy' where fuzzy(Body = 'connection', 0.79) order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q50 task=top_k filter=like freq=mid (prefix wildcard conn%)
from index 'Logs/Search' where search(Body, 'conn*') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q51 task=top_k filter=phrase,or freq=hi (phrase OR term)
from index 'Logs/Search' where search(Body, '"failed to place order"') or search(Body, 'charge') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q52 task=top_k filter=and,negation freq=hi (error but NOT cache)
from index 'Logs/Search' where search(Body, 'error') and not search(Body, 'cache') order by score() desc select Timestamp, ServiceName, Body limit 100
-- Q53 task=top_k filter=and,window freq=hi (term + service + Timestamp BETWEEN 6h)
from index 'Logs/Fuzzy' where ServiceName = 'payment' and search(Body, 'charge') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' order by score() desc select Timestamp, ServiceName, Body limit 100

-- Q54 task=group_by filter=or freq=hi (key=SeverityText, ordered)
from index 'Logs/Search' where search(Body, 'error failed') select facet(SeverityText)
-- Q55 task=group_by filter=term freq=hi (key=SeverityText, ordered)
from index 'Logs/Search' where search(Body, 'charge') select facet(SeverityText)
-- Q56 task=group_by filter=and freq=hi (key=SeverityText, NO order by)
from index 'Logs/Search' where search(Body, 'failed order', and) select facet(SeverityText)
-- Q57 task=group_by filter=or freq=hi (key=ScopeName, top 20 ordered)
from index 'Logs/Search' where search(Body, 'error failed') select facet(ScopeName, $top20)
-- Q58 task=group_by filter=regexp freq=hi (key=ScopeName)
from index 'Logs/Search' where search(Body, 'charg*') select facet(ScopeName, $top20)
-- Q59 task=group_by filter=fuzzy freq=mid (key=ScopeName)
from index 'Logs/Fuzzy' where fuzzy(Body = 'connection', 0.89) select facet(ScopeName, $top20)
-- Q60 task=group_by filter=or freq=hi (key=ScopeName, NO order by)
from index 'Logs/Search' where search(Body, 'error failed') select facet(ScopeName)
-- Q61 task=group_by filter=term freq=hi (key=minute)
from index 'Logs/Search' where search(Body, 'error') select facet(TimestampMinute)
-- Q62 task=group_by filter=and freq=hi (key=minute)
from index 'Logs/Search' where search(Body, 'failed order', and) select facet(TimestampMinute)
-- Q63 task=group_by filter=phrase freq=mid (key=minute)
from index 'Logs/Search' where search(Body, '"failed to place order"') select facet(TimestampMinute)
-- Q64 task=group_by filter=or freq=hi (key=minute)
from index 'Logs/Search' where search(Body, 'error failed charge') select facet(TimestampMinute)
-- Q65 task=group_by filter=term freq=hi (key=minute, NO order by)
from index 'Logs/Search' where search(Body, 'error') select facet(TimestampMinute)
-- Q66 task=group_by filter=and freq=hi (key=SeverityText, Body term + indexed service)
from index 'Logs/Search' where ServiceName = 'frontend' and search(Body, 'failed') select facet(SeverityText)
-- Q67 task=group_by filter=or freq=hi (two keys: SeverityText, ScopeName)
from index 'Logs/Search' where search(Body, 'error failed') select facet(SeverityScope, $top20)

-- Q68 task=recent filter=and,window freq=hi (recent failed-order logs from checkout)
from index 'Logs/Search' where ServiceName = 'checkout' and search(Body, 'failed order', and) and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q69 task=recent filter=or,window freq=hi (recent error/failed/charge, severity>=warn)
from index 'Logs/Search' where search(Body, 'error failed charge') and SeverityNumber >= 13 and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q70 task=recent filter=term,window freq=hi (recent error logs in a 6h window)
from index 'Logs/Search' where search(Body, 'error') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q71 task=recent filter=phrase,window freq=mid (recent 'failed to place order')
from index 'Logs/Search' where search(Body, '"failed to place order"') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q72 task=recent filter=and,window freq=hi (recent payment charges)
from index 'Logs/Search' where ServiceName = 'payment' and search(Body, 'charge') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q73 task=recent filter=window (pure time-series tail: recent cart logs, no text search)
from index 'Logs/Search' where ServiceName = 'cart' and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q74 task=recent filter=regexp,window freq=hi (recent charg* logs in a 6h window)
from index 'Logs/Search' where search(Body, 'charg*') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q75 task=recent filter=or,window freq=mid (recent connection/request/conversion logs)
from index 'Logs/Search' where search(Body, 'connection request conversion') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' order by Timestamp desc select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q76 task=recent filter=and,window freq=hi (checkout failed&order, NO order by)
from index 'Logs/Search' where ServiceName = 'checkout' and search(Body, 'failed order', and) and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q77 task=recent filter=or,window freq=hi (error/failed/charge sev>=warn, NO order by)
from index 'Logs/Search' where search(Body, 'error failed charge') and SeverityNumber >= 13 and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q78 task=recent filter=term,window freq=hi (error 6h, NO order by)
from index 'Logs/Search' where search(Body, 'error') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q79 task=recent filter=phrase,window freq=mid (failed to place order 6h, NO order by)
from index 'Logs/Search' where search(Body, '"failed to place order"') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q80 task=recent filter=and,window freq=hi (payment charges, NO order by)
from index 'Logs/Search' where ServiceName = 'payment' and search(Body, 'charge') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q81 task=recent filter=window (cart tail, NO order by, no text search)
from index 'Logs/Search' where ServiceName = 'cart' and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q82 task=recent filter=regexp,window freq=hi (charg.* 6h, NO order by)
from index 'Logs/Search' where search(Body, 'charg*') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T06:00:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100
-- Q83 task=recent filter=or,window freq=mid (connection/request/conversion, NO order by)
from index 'Logs/Search' where search(Body, 'connection request conversion') and Timestamp between '2025-09-23T00:00:00.0000000Z' and '2025-09-23T00:30:00.0000000Z' select Timestamp, ServiceName, SeverityText, Body limit 100

-- Q84 task=join filter=term freq=hi (frontend 'failed' traces that also involve payment)
from index 'Logs/Search' where ServiceName = 'frontend' and search(Body, 'failed') and HasPayment = true select distinct TraceId limit 0, 0
-- Q85 task=join filter=or freq=hi
from index 'Logs/Search' where search(Body, 'error failed') and HasPayment = true select distinct TraceId limit 0, 0
-- Q86 task=join filter=phrase freq=mid
from index 'Logs/Search' where search(Body, '"failed to place order"') and HasPayment = true select distinct TraceId limit 0, 0
-- Q87 task=join filter=regexp freq=hi
from index 'Logs/Search' where search(Body, 'charg*') and HasFrontend = true select distinct TraceId limit 0, 0
-- Q88 task=join filter=and freq=hi (failed&order traces that also involve cart)
from index 'Logs/Search' where search(Body, 'failed order', and) and HasCart = true select distinct TraceId limit 0, 0
-- Q89 task=join filter=or freq=mid
from index 'Logs/Search' where search(Body, 'connection request') and HasFrontend = true select distinct TraceId limit 0, 0
-- Q90 task=join filter=and freq=hi
from index 'Logs/Search' where search(Body, 'charge request', and) and HasFrontend = true select distinct TraceId limit 0, 0
-- Q91 task=join filter=term freq=hi
from index 'Logs/Search' where search(Body, 'order') and HasPayment = true select distinct TraceId limit 0, 0
-- Q92 task=join filter=prefix freq=hi
from index 'Logs/Search' where search(Body, 'charg*') and HasFrontend = true select distinct TraceId limit 0, 0
