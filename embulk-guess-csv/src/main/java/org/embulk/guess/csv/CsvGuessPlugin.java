/*
 * Copyright 2021 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.guess.csv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.OptionalDouble;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.GuessPlugin;

public class CsvGuessPlugin implements GuessPlugin {
    @Override
    public ConfigDiff guess(final ConfigSource config, final Buffer sample) {
        return this.guessLines(config, LineGuessHelper.toLines(config, sample));
    }

    private ConfigDiff guessLines(final ConfigSource config, final List<String> sampleLines) {
        final ConfigDiff configDiff = Exec.newConfigDiff();

        // return {} unless config.fetch("parser", {}).fetch("type", "csv") == "csv"
        if (!"csv".equals(config.getNestedOrGetEmpty("parser").get(String.class, "type", "csv"))) {
            return configDiff;
        }

        final ConfigSource parserConfig = config.getNestedOrGetEmpty("parser");
        final String delim;

        // if parser_config["type"] == "csv" && parser_config["delimiter"]
        if ("csv".equals(parserConfig.get(String.class, "type", "csv")) && parserConfig.has("delimiter")) {
            delim = parserConfig.get(String.class, "delimiter");
        } else {
            delim = this.guessDelimiter(sampleLines);
        }

        final ConfigDiff parserGuessed = Exec.newConfigDiff();
        parserGuessed.merge(parserConfig);
        parserGuessed.set("type", "csv");
        parserGuessed.set("delimiter", delim);

        if (!parserGuessed.has("quote")) {
            final String quote = guessQuote(sampleLines, delim);
            parserGuessed.set("quote", quote);  // quote may be null.
        }
        // setting '' is not allowed any more. this line converts obsoleted config syntax to explicit syntax.
        if ("".equals(parserGuessed.get(String.class, "quote"))) {
            parserGuessed.set("quote", "\"");
        }

        if (!parserGuessed.has("escape")) {
            final String quote = parserGuessed.get(String.class, "quote");
            if (quote != null) {
                final String escape = guessEscape(sampleLines, delim, quote);
                parserGuessed.set("escape", escape);  // escape may be null.
            } else {
                // escape does nothing if quote is disabled
            }
        }

        if (!parserGuessed.has("null_string")) {
            final String nullString = guessNullString(sampleLines, delim);
            if (nullString != null) {
                parserGuessed.set("null_string", nullString);
            }
            // don't even set null_string to avoid confusion of null and 'null' in YAML format
        }

        // guessing skip_header_lines should be before guessing guess_comment_line_marker
        // because lines supplied to CsvTokenizer already don't include skipped header lines.
        // skipping empty lines is also disabled here because skipping header lines is done by
        // CsvParser which doesn't skip empty lines automatically

        /*
        sample_records = split_lines(parser_guessed, false, sample_lines, delim, {})
        skip_header_lines = guess_skip_header_lines(sample_records)
        sample_lines = sample_lines[skip_header_lines..-1]
        sample_records = sample_records[skip_header_lines..-1]

        unless parser_guessed.has_key?("comment_line_marker")
          comment_line_marker, sample_lines =
            guess_comment_line_marker(sample_lines, delim, parser_guessed["quote"], parser_guessed["null_string"])
          if comment_line_marker
            parser_guessed["comment_line_marker"] = comment_line_marker
          end
        end

        sample_records = split_lines(parser_guessed, true, sample_lines, delim, {})

        # It should fail if CSV parser cannot parse sample_lines.
        if sample_records.nil? || sample_records.empty?
          return {}
        end

        if sample_lines.size == 1
          # The file contains only 1 line. Assume that there are no header line.
          header_line = false

          column_types = SchemaGuess.types_from_array_records(sample_records[0, 1])

          unless parser_guessed.has_key?("trim_if_not_quoted")
            sample_records_trimmed = split_lines(parser_guessed, true, sample_lines, delim, {"trim_if_not_quoted" => true})
            column_types_trimmed = SchemaGuess.types_from_array_records(sample_records_trimmed)
            if column_types != column_types_trimmed
              parser_guessed["trim_if_not_quoted"] = true
              column_types = column_types_trimmed
            else
              parser_guessed["trim_if_not_quoted"] = false
            end
          end
        else
          # The file contains more than 1 line. If guessed first line's column types are all strings or boolean, and the types are
          # different from the other lines, assume that the first line is column names.
          first_types = SchemaGuess.types_from_array_records(sample_records[0, 1])
          other_types = SchemaGuess.types_from_array_records(sample_records[1..-1] || [])

          unless parser_guessed.has_key?("trim_if_not_quoted")
            sample_records_trimmed = split_lines(parser_guessed, true, sample_lines, delim, {"trim_if_not_quoted" => true})
            other_types_trimmed = SchemaGuess.types_from_array_records(sample_records_trimmed[1..-1] || [])
            if other_types != other_types_trimmed
              parser_guessed["trim_if_not_quoted"] = true
              other_types = other_types_trimmed
            else
              parser_guessed["trim_if_not_quoted"] = false
            end
          end

          header_line = (first_types != other_types && first_types.all? {|t| ["string", "boolean"].include?(t) }) || guess_string_header_line(sample_records)
          column_types = other_types
        end

        if column_types.empty?
          # TODO here is making the guessing failed if the file doesn't contain any columns. However,
          #      this may not be convenient for users.
          return {}
        end

        if header_line
          parser_guessed["skip_header_lines"] = skip_header_lines + 1
        else
          parser_guessed["skip_header_lines"] = skip_header_lines
        end

        parser_guessed["allow_extra_columns"] = false unless parser_guessed.has_key?("allow_extra_columns")
        parser_guessed["allow_optional_columns"] = false unless parser_guessed.has_key?("allow_optional_columns")

        if header_line
          column_names = sample_records.first.map(&:strip)
        else
          column_names = (0..column_types.size).to_a.map {|i| "c#{i}" }
        end
        schema = []
        column_names.zip(column_types).each do |name,type|
          if name && type
            schema << new_column(name, type)
          end
        end
        parser_guessed["columns"] = schema
 */

        configDiff.setNested("parser", parserGuessed);
        return configDiff;
    }

    /*
      def new_column(name, type)
        if type.is_a?(SchemaGuess::TimestampTypeMatch)
          {"name" => name, "type" => type, "format" => type.format}
        else
          {"name" => name, "type" => type}
        end
      end
    */

    private static List<List<String>> splitLines(
            final ConfigDiff parserConfig,
            final boolean skipEmptyLines,
            final List<String> sampleLines,
            final String delim,
            final Boolean trimIfNotQuoted) {
        try {
            final String nullString = parserConfig.get(String.class, "null_string");
            final ConfigDiff config = Exec.newConfigDiff();
            if (trimIfNotQuoted != null) {
                config.set("trim_if_not_quoted", (boolean) trimIfNotQuoted);
            }
            config.set("charset", "UTF-8");
            config.set("columns", new ArrayList<>());

            // parser_task = config.load_config(org.embulk.standards.CsvParserPlugin::PluginTask)

            /*
            data = sample_lines.map {|line| line.force_encoding("UTF-8") }.join(parser_task.getNewline.getString.encode("UTF-8"))
            */

            /*
        sample = Buffer.from_ruby_string(data)
        decoder = Java::LineDecoder.new(Java::ListFileInput.new([[sample.to_java]]), parser_task)
        tokenizer = org.embulk.standards.CsvTokenizer.new(decoder, parser_task)
        rows = []
        while tokenizer.nextFile
          while tokenizer.nextRecord(skip_empty_lines)
            begin
              columns = []
              while true
                begin
                  column = tokenizer.nextColumn
                  quoted = tokenizer.wasQuotedColumn
                  if null_string && !quoted && column == null_string
                    column = nil
                  end
                  columns << column
                rescue org.embulk.standards.CsvTokenizer::TooFewColumnsException
                  rows << columns
                  break
                end
              end
            rescue org.embulk.standards.CsvTokenizer::InvalidValueException
              # TODO warning
              tokenizer.skipCurrentLine
            end
          end
        end
        return rows
            */
        } catch (final RuntimeException ex) {
            // TODO warning if fallback to this ad-hoc implementation
            //sample_lines.map {|line| line.split(delim) }
        }
        return null;
    }

    private String guessDelimiter(final List<String> sampleLines) {
        String selectedDelimiter = null;
        double mostWeight = 0.0;
        for (final char delimiter : DELIMITER_CANDIDATES) {
            final List<Integer> counts = StreamSupport.stream(sampleLines.spliterator(), false)
                    .map(line -> (int) (line.chars().filter(c -> c == delimiter).count()))
                    .collect(Collectors.toList());
            final int total = sumOfList(counts);
            if (total > 0) {
                final double weight = total / standardDeviationOfList(counts);
                if (weight > mostWeight) {
                    selectedDelimiter = "" + delimiter;
                    mostWeight = weight;
                }
            }
        }

        if (selectedDelimiter != null && mostWeight > 1.0) {
            return selectedDelimiter;
        }
        return "" + DELIMITER_CANDIDATES.get(0);  // assuming single column CSV
    }

    private static String guessQuote(final List<String> sampleLines, final String delim) {
        String selectedQuote = null;
        double mostWeight = 0.0;

        final String delimRegex = Pattern.quote(delim);
        for (final char q : QUOTE_CANDIDATES) {
            final String qRegex = Pattern.quote("" + q);
            final List<Integer> weights = new ArrayList<>();
            for (final String line : sampleLines) {
                final long count = line.chars().filter(c -> c == q).count();
                if (count > 0L) {
                    weights.add((int) count + weighQuote(line, delimRegex, qRegex));
                }
            }
            final double weight = (weights.isEmpty() ? 0.0 : averageOfList(weights));
            if (weight > mostWeight) {
                selectedQuote = "" + q;
                mostWeight = weight;
            }
        }
        if (mostWeight >= 10.0) {
            return selectedQuote;
        }

        if (!guessForceNoQuote(sampleLines, delim, "\"")) {
            // assuming CSV follows RFC for quoting
            return "\"";
        }
        // disable quoting (set null)
        return null;
    }

    private static boolean guessForceNoQuote(final List<String> sampleLines, final String delim, final String quoteCandidate) {
        final String delimRegex = Pattern.quote(delim);
        final String qRegex = Pattern.quote(quoteCandidate);
        final Pattern pattern = Pattern.compile(String.format("(?:\\A|%s)\\s*[^%s]+%s", delimRegex, qRegex, qRegex));
        for (final String line : sampleLines) {
            // quoting character appear at the middle of a non-quoted value
            if (pattern.matcher(line).matches()) {
                return true;
            }
        }
        return false;
    }

    private static String guessEscape(final List<String> sampleLines, final String delim, final String quote) {
        int maxCount = 0;
        String selectedEscape = null;
        for (final String str : ESCAPE_CANDIDATES) {
            final Pattern regex = Pattern.compile(String.format(
                    "%s(?:%s|%s)", Pattern.quote(str), Pattern.quote(delim), Pattern.quote(quote)));
            final int count = StreamSupport.stream(sampleLines.spliterator(), false)
                    .mapToInt(line -> (int) (countPattern(line, regex)))
                    .sum();
            if (count > maxCount) {
                selectedEscape = str;
                maxCount = count;
            }
        }

        if (selectedEscape == null) {
            if ("\"".equals(quote)) {
                // assuming this CSV follows RFC for escaping
                return "\"";
            } else {
                // disable escaping (set null)
                return null;
            }
        }

        return selectedEscape;
    }

    private static String guessNullString(final List<String> sampleLines, final String delim) {
        int maxCount = 0;
        String selectedNullString = null;
        for (final String str : NULL_STRING_CANDIDATES) {
            final Pattern regex = Pattern.compile(String.format(
                    "(?:^|%s)%s(?:$|%s)", Pattern.quote(delim), Pattern.quote(str), Pattern.quote(delim)));
            final int count = StreamSupport.stream(sampleLines.spliterator(), false)
                    .mapToInt(line -> (int) (countPattern(line, regex)))
                    .sum();
            if (count > maxCount) {
                selectedNullString = str;
                maxCount = count;
            }
        }

        return selectedNullString;
    }

    /*
    private
      def guess_skip_header_lines(sample_records)
        counts = sample_records.map {|records| records.size }
        (1..[MAX_SKIP_LINES, counts.length - 1].min).each do |i|
          check_row_count = counts[i-1]
          if counts[i, NO_SKIP_DETECT_LINES].all? {|c| c <= check_row_count }
            return i - 1
          end
        end
        return 0
      end

    */
    /*
    private
      def guess_comment_line_marker(sample_lines, delim, quote, null_string)
        exclude = []
        exclude << /^#{Regexp.escape(quote)}/ if quote && !quote.empty?
        exclude << /^#{Regexp.escape(null_string)}(?:#{Regexp.escape(delim)}|$)/ if null_string

        guessed = COMMENT_LINE_MARKER_CANDIDATES.map do |str|
          regexp = /^#{Regexp.quote(str)}/
          unmatch_lines = sample_lines.reject do |line|
            exclude.all? {|ex| line !~ ex } && line =~ regexp
          end
          match_count = sample_lines.size - unmatch_lines.size
          [str, match_count, unmatch_lines]
        end.select {|str,match_count,unmatch_lines| match_count > 0 }.sort_by {|str,match_count,unmatch_lines| -match_count }

        str, match_count, unmatch_lines = guessed.first
        if str
          return str, unmatch_lines
        else
          return nil, sample_lines
        end
      end
    */
    /*
    private
      def guess_string_header_line(sample_records)
        first = sample_records.first
        first.count.times do |column_index|
          lengths = sample_records.map {|row| row[column_index] }.compact.map {|v| v.to_s.size }
          if lengths.size > 1
            if array_variance(lengths[1..-1]) <= 0.2
              avg = array_avg(lengths[1..-1])
              if avg == 0.0 ? lengths[0] > 1 : (avg - lengths[0]).abs / avg > 0.7
                return true
              end
            end
          end
        end
        return false
      end
    */

    private static int sumOfList(final List<Integer> integers) {
        return StreamSupport.stream(integers.spliterator(), false).mapToInt(i -> i).sum();
    }

    private static double averageOfList(final List<Integer> integers) {
        return StreamSupport.stream(integers.spliterator(), false).mapToInt(i -> i).average().orElse(0.0);
    }

    private double varianceOfList(final List<Integer> integers) {
        final double average = averageOfList(integers);
        return StreamSupport.stream(integers.spliterator(), false)
                .mapToDouble(i -> (((double) i) - average) * (((double) i) - average))
                .average()
                .orElse(0.0);
    }

    private double standardDeviationOfList(final List<Integer> integers) {
        final double result = Math.sqrt(varianceOfList(integers));
        if (result < (double) 0.00000000001) {  // result must be >= 0
            return 0.000000001;
        }
        return result;
    }

    private static int weighQuote(final String line, final String delimRegex, final String qRegex) {
        final Pattern patternQ = Pattern.compile(String.format(
                "(?:\\A|%s)\\s*%s(?:(?!%s).)*\\s*%s(?:$|%s)",
                delimRegex, qRegex, qRegex, qRegex, delimRegex));

        final Pattern patternD = Pattern.compile(String.format(
                "(?:\\A|%s)\\s*%s(?:(?!%s).)*\\s*%s(?:$|%s)",
                delimRegex, qRegex, delimRegex, qRegex, delimRegex));

        return countPattern(line, patternQ) * 20 + countPattern(line, patternD) * 40;
    }

    private static int countPattern(final String string, final Pattern pattern) {
        final Matcher matcher = pattern.matcher(string);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private static List<Character> DELIMITER_CANDIDATES = Collections.unmodifiableList(Arrays.asList(
            ',',
            '\t',
            '|',
            ';'
    ));

    private static List<Character> QUOTE_CANDIDATES = Collections.unmodifiableList(Arrays.asList(
            '\"',
            '\''
    ));

    private static List<String> ESCAPE_CANDIDATES = Collections.unmodifiableList(Arrays.asList(
            "\\",
            "\""
    ));

    private static List<String> NULL_STRING_CANDIDATES = Collections.unmodifiableList(Arrays.asList(
            "null",
            "NULL",
            "#N/A",
            "\\N"  // MySQL LOAD, Hive STORED AS TEXTFILE
    ));

    private static List<String> COMMENT_LINE_MARKER_CANDIDATES = Collections.unmodifiableList(Arrays.asList(
            "#",
            "//"
    ));

    private static int MAX_SKIP_LINES = 10;
    private static int NO_SKIP_DETECT_LINES = 10;
}
