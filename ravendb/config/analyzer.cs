// Custom analyzer for the Body field: split on non-alphanumeric, lowercase.
//
// WHY NOT A BUILT-IN ONE
// ----------------------
// The shared spec is what every other adapter implements -- split on
// non-alphanumeric characters, lowercase, no stemming, no stopwords, positions
// kept for phrase queries (elastic/config/index_mapping.json char_group +
// lowercase; clickhouse splitByNonAlpha + lower; serenedb split_by_non_alpha;
// postgres to_tsvector('simple')). None of RavenDB's eight built-ins matches it:
//
//   RavenStandardAnalyzer   StandardTokenizer + RavenStandardFilter, and that
//                           filter carries StopAnalyzer.ENGLISH_STOP_WORDS_SET.
//                           It drops the "to" out of "failed to place order",
//                           which is the phrase behind Q11/Q12/Q14/Q15 -- all
//                           task=count, the queries lib/check_results.py
//                           compares as exact integers. Using it would return a
//                           different number than every other engine, and a
//                           smaller index that looks faster for the wrong reason.
//   StandardAnalyzer        Same stopword set; also keeps hostnames and email
//                           addresses as single tokens rather than splitting.
//   SimpleAnalyzer          LetterTokenizer drops DIGITS, breaking Q12's
//                           "post to email service expected 200 got 500".
//   LowerCaseKeywordAnalyzer / KeywordAnalyzer   Whole field as one token.
//   WhitespaceAnalyzer / LowerCaseWhitespaceAnalyzer   No punctuation split,
//                           so "dogs," stays attached to its comma.
//   StopAnalyzer            Stopwords again.
//   NGramAnalyzer           Character n-grams, a different index entirely.
//
// So the adapter ships this one. It is the direct equivalent of Lucene's
// LowerCaseTokenizer except that IsTokenChar accepts digits as well as letters,
// which is what makes it match splitByNonAlpha rather than LetterTokenizer.
//
// Installed by ./load via PUT /databases/<db>/admin/analyzers, then referenced
// from config/index.json by the fully-qualified name below.

using System.IO;
using Lucene.Net.Analysis;

namespace SearchBench
{
    public class NonAlphanumericLowerCaseAnalyzer : Analyzer
    {
        public override TokenStream TokenStream(string fieldName, TextReader reader)
        {
            return new NonAlphanumericLowerCaseTokenizer(reader);
        }

        // Reuse the tokenizer across documents. Without this every indexed field
        // allocates a fresh tokenizer, which is measurable over a billion docs.
        public override TokenStream ReusableTokenStream(string fieldName, TextReader reader)
        {
            var tokenizer = PreviousTokenStream as Tokenizer;
            if (tokenizer == null)
            {
                tokenizer = new NonAlphanumericLowerCaseTokenizer(reader);
                PreviousTokenStream = tokenizer;
            }
            else
            {
                tokenizer.Reset(reader);
            }

            return tokenizer;
        }
    }

    public class NonAlphanumericLowerCaseTokenizer : CharTokenizer
    {
        public NonAlphanumericLowerCaseTokenizer(TextReader reader)
            : base(reader)
        {
        }

        // A token is a run of letters or digits; everything else is a delimiter.
        // Letters OR digits, not Lucene's letters-only, so "500" survives.
        protected override bool IsTokenChar(char c)
        {
            return char.IsLetterOrDigit(c);
        }

        // Case folding happens here, character by character, as the tokenizer
        // reads -- the same place the other engines' lowercase filter runs.
        protected override char Normalize(char c)
        {
            return char.ToLowerInvariant(c);
        }
    }
}
