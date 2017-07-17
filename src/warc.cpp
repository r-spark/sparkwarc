// [[Rcpp::plugins("cpp11")]]

#include <Rcpp.h>
using namespace Rcpp;

#include <stdio.h>
#include <zlib.h>
#include <regex>

// [[Rcpp::export]]
List rcpp_hello_world() {

  CharacterVector x = CharacterVector::create( "foo", "bar" )  ;
  NumericVector y   = NumericVector::create( 0.0, 1.0 ) ;
  List z            = List::create( x, y ) ;

  return z ;
}

// [[Rcpp::export]]
DataFrame rcpp_read_warc(std::string path,
                         std::string filter,
                         std::string include) {
  try {
    std::regex filter_regex(filter);
    std::regex include_regex(include);

    FILE *fp = fopen(path.c_str(), "rb");
    if (!fp) Rcpp::stop("Failed to open WARC file.");

    gzFile gzf = gzdopen(fileno(fp), "rb");
    if (!gzf) Rcpp::stop("Failed to open WARC as a compressed file.");

    const int buffer_size = 4 * 1024;
    char buffer[buffer_size] = {'\0'};

    std::list<std::string> warc_entries;

    const int warc_mean_size = 40 * 1024;
    std::string warc_entry;
    warc_entry.reserve(warc_mean_size);

    bool one_matched = false;
    const std::string warc_separator = "WARC/1.0";

    long stats_tags_total = 0;
    const std::regex_constants::regex stats_tags_regex("<.+>");
    std::list<long> warc_stats;

    while(gzgets(gzf, buffer, buffer_size) != Z_NULL) {
      std::string line(buffer);
      std::string no_newline = line.substr(0, line.find_first_of('\n'));

      if (!filter.empty() && !one_matched) {
        one_matched = regex_match(no_newline, filter_regex);
      }

      if (std::string(line).substr(0, warc_separator.size()) == warc_separator && warc_entry.size() > 0) {
        if (filter.empty() || one_matched) {
          warc_entries.push_back(warc_entry);
          warc_stats.push_back(stats_tags_total);
          stats_tags_total = 0;
        }

        one_matched = false;

        warc_entry.clear();
      }

      if (include.empty() || regex_match(no_newline, include_regex)) {
        warc_entry.append(line);
      }

      std::smatch stats_tags_match;
      if (regex_search(no_newline, stats_tags_match, stats_tags_regex)) {
        stats_tags_total += stats_tags_match.size();
      }
    }

    if (gzf) gzclose(gzf);
    if (fp) fclose(fp);

    long idxEntry = 0;
    CharacterVector results(warc_entries.size());
    std::for_each(warc_entries.begin(), warc_entries.end(), [&results, &idxEntry](std::string &entry) {
      results[idxEntry++] = entry;
    });

    long idxStat = 0;
    NumericVector stats(warc_stats.size());
    std::for_each(warc_stats.begin(), warc_stats.end(), [&stats, &idxStat](long &stat) {
      stats[idxStat++] = stat;
    });

    return DataFrame::create(Named("tags") = stats, _["content"] = results);
  }
  catch (const std::regex_error& e) {
    Rcpp::stop(e.what());
  }
}
