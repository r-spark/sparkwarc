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
CharacterVector rcpp_read_warc(std::string path,
                               std::string filter) {
  std::regex filter_regex(filter);

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
  while(gzgets(gzf, buffer, buffer_size) != Z_NULL) {
    std::string line(buffer);

    if (filter.size() > 0 && !one_matched) {
      std::string no_newline = line.substr(0, line.find_first_of('\n'));
      one_matched = regex_match(no_newline, filter_regex);
    }

    if (std::string(line).substr(0, warc_separator.size()) == warc_separator && warc_entry.size() > 0) {
      if (filter.size() == 0 || one_matched) {
        warc_entries.push_back(warc_entry);
      }

      one_matched = false;
      warc_entry.clear();
    }

    warc_entry.append(line);
  }

  if (gzf) gzclose(gzf);
  if (fp) fclose(fp);

  CharacterVector results(warc_entries.size());

  long idxEntry = 0;
  std::for_each(warc_entries.begin(), warc_entries.end(), [&results, &idxEntry](std::string &entry) {
    results[idxEntry++] = entry;
  });

  return results;
}
