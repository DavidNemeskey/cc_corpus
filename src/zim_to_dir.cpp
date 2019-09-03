#include <arpa/inet.h>
#include <fstream>
#include <iostream>
#include <map>
// Needs C++17
#include <filesystem>

#include "cxxopts.hpp"
#include "zstr.hpp"
#include <zim/file.h>
#include <zim/fileiterator.h>

namespace fs = std::filesystem;

/** Holds disambiguation patterns in titles for languages we support. */
std::map<std::string, std::string> disambig = {
    {"hu", "(egyértelműsítő lap)"}, {"en", "(disambiguation)"}
};

/** Keeps Options alive so that the returned ParseResult is valid. */
class ArgumentParser {
public:
    ArgumentParser(char* argv[]) {
        try {
            options = std::unique_ptr<cxxopts::Options>(new cxxopts::Options(
                argv[0], "Converts a static Wikipedia HTML dump in a .zim file "
                         "to a directory of files. Each file contains a list "
                         "of uint32_t-string tuples, the first being the "
                         "number of characters in the latter."
            ));
            options
                ->add_options()
                ("i,input-file", "the name of the source .zim file",
                 cxxopts::value<std::string>())
                ("o,output-dir", "the name of the output directory",
                 cxxopts::value<std::string>())
                ("l,language", "the two-letter language code of the Wikipedia dump",
                 cxxopts::value<std::string>()->default_value("hu"))
                ("d,documents", "the number of articles saved into a "
                                "single output file",
                 cxxopts::value<size_t>()->default_value("2500"))
                ("Z,zeroes", "he number of zeroes in the output files' names.",
                 cxxopts::value<size_t>()->default_value("4"))
                ("h,help", "print help")
            ;
        } catch (const cxxopts::OptionException& e) {
            std::cerr << "Error parsing options: " << e.what() << std::endl;
            exit(1);
        }
    }

    cxxopts::ParseResult parse(int argc, char* argv[]) {
        try { 
            auto args = options->parse(argc, argv);
            if (args.count("help")) {
                std::cout << options->help({""}) << std::endl;
                exit(0);
            }
            if (!args.count("input-file") || !args.count("output-dir")) {
                std::cout << "Both -i and -o must be specified." << std::endl;
                exit(1);
            }
            std::string language = args["language"].as<std::string>();
            std::vector<std::string> v = {"aha", "baha"};
            if (!disambig.count(language)) {
                std::cout << "Language '" << language << "' is no supported. "
                          << "Choose between 'en' and 'hu'." << std::endl;
            }
            return args;
        } catch (const cxxopts::OptionException& e) {
            std::cerr << "Error parsing options: " << e.what() << std::endl;
            exit(1);
        }
    }

private:
    std::unique_ptr<cxxopts::Options> options;
};

/** Writes a batch of consecutively numbered documents. */
struct BatchWriter {
    BatchWriter(const std::string& output_dir, size_t documents, size_t zeroes) :
        output_dir_(output_dir), documents_(documents), zeroes_(zeroes),
        curr_num_(0), written_(documents) {
    }

    /**
     * Writes the article represented by \c blob to the currently open file.
     *
     * Increases the file number if necessary.
     */
    void write(const zim::Blob& blob) {
        if (written_ == documents_) {
            std::string num = std::to_string(++curr_num_);
            num = std::string(zeroes_ - num.length(), '0') + num + ".htmls.gz";
            out_.reset(new zstr::ofstream(output_dir_ / num, std::ios::out | std::ios::binary));
            written_ = 0;
        }
        uint32_t size = htonl(static_cast<uint32_t>(blob.size()));
        out_->write(reinterpret_cast<char*>(&size), sizeof(size));
        out_->write(blob.data(), blob.size());
        written_++;
    }

    /** The output directory. */
    fs::path output_dir_;
    /** The number of documents to write to a file. */
    size_t documents_;
    /** The minimum number of digits in a file's name. */
    size_t zeroes_;
    /** The number of the current file. */
    size_t curr_num_;
    /** The output file being currently written. */
    std::unique_ptr<zstr::ofstream> out_;
    /** How many documents have been written to the current file. */
    size_t written_;
};

int main(int argc, char* argv[]) {
    ArgumentParser parser(argv);
    auto args = parser.parse(argc, argv);
    BatchWriter bw(args["output-dir"].as<std::string>(),
                   args["documents"].as<size_t>(), args["zeroes"].as<size_t>());

    try {
        zim::File f(args["input-file"].as<std::string>());
        fs::create_directory(args["output-dir"].as<std::string>());

        size_t doc_no = 0;
        std::string& pattern = disambig[args["language"].as<std::string>()];
        for (zim::File::const_iterator it = f.begin(); it != f.end(); ++it) {
            std::string title = it->getTitle();
            if (it->getNamespace() != 'A') {
                std::cerr << "Dropping article " << title
                          << " not in namespace A..." <<  std::endl;
            } else if (it->isRedirect()) {
                std::cerr << "Dropping redirect article " << title
                          << "..." <<  std::endl;
            } else if (it->isDeleted()) {
                std::cerr << "Dropping deleted article " << title
                          << "..." <<  std::endl;
            } else if (it->getTitle().find(pattern) != std::string::npos) {
                std::cerr << "Dropping disambiguation article " << title
                          << "..." <<  std::endl;
            } else {
                if (++doc_no % 1000 == 0) {
                    std::cerr << "At the " << doc_no << "th document." << std::endl;
                }
                std::cerr << "Writing articule " << title << "..." << std::endl;
                bw.write(it->getData());
            }
        }
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
