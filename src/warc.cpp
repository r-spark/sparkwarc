#include <Rcpp.h>
using namespace Rcpp;

#include <stdio.h>
#include <zlib.h>

// [[Rcpp::export]]
List rcpp_hello_world() {

  CharacterVector x = CharacterVector::create( "foo", "bar" )  ;
  NumericVector y   = NumericVector::create( 0.0, 1.0 ) ;
  List z            = List::create( x, y ) ;

  return z ;
}

std::size_t rcpp_find_tag(std::string line, std::size_t pos) {
  std::size_t tag_start = line.find("<", pos);
  if (tag_start != std::string::npos && line.find(">", tag_start + 1)) {
    return tag_start;
  }

  return std::string::npos;
}

// [[Rcpp::export]]
DataFrame rcpp_read_warc(std::string path,
                         std::string filter,
                         std::string include) {

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
  std::list<long> warc_stats;

  while(gzgets(gzf, buffer, buffer_size) != Z_NULL) {
    std::string line(buffer);

    if (!filter.empty() && !one_matched) {
      one_matched = line.find(filter) != std::string::npos;
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

    if (include.empty() || line.find(include) != std::string::npos) {
      warc_entry.append(line);
    }

    std::size_t tag_start = rcpp_find_tag(line, 0);
    while(tag_start != std::string::npos) {
      stats_tags_total += 1;
      tag_start = rcpp_find_tag(line, tag_start + 1);
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
