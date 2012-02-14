/****************************************************************************
 *
 * Command line arguments:
 *
 * upload TABLEID PREFIX
 * train TABLEID RESOLUTION START STOP
 * learn TABLEID START STOP
 * query TABLEID IMAGEID START STOP
 *
 ****************************************************************************/


#include "aws.h"
#include "yale.h"

#include <iostream>
#include <cstdlib>


///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

static const char *UPLOAD_CMD = "upload";
static const char *TRAIN_CMD = "train";
static const char *LEARN_CMD = "learn";
static const char *QUERY_CMD = "query";

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

int
main(const int argc, const char **argv)
{
	if (argc < 2) {
		std::cerr << "Usage error";
		return EXIT_FAILURE;
	}

	int rc = EXIT_SUCCESS;
	const char *cmd = argv[1];
	if (!strcmp(cmd, UPLOAD_CMD)) {
		if (argc < 4) {
			std::cerr << "Usage error: wrong number of arguments" << std::endl;
			return EXIT_FAILURE;
		}
	} else if (!strcmp(cmd, TRAIN_CMD)) {
		if (argc < 6) {
			std::cerr << "Usage error: wrong number of arguments" << std::endl;
			return EXIT_FAILURE;
		}
	} else if (!strcmp(cmd, LEARN_CMD)) {
		if (argc < 5) {
			std::cerr << "Usage error: wrong number of arguments" << std::endl;
			return EXIT_FAILURE;
		}
	} else if (!strcmp(cmd, QUERY_CMD)) {
		if (argc < 6) {
			std::cerr << "Usage error: wrong number of arguments" << std::endl;
			return EXIT_FAILURE;
		}
	} else {
		std::cerr << "Usage error: unknown command (" << cmd << ")"
				<< std::endl;
		return EXIT_FAILURE;
	}

	CVDB cvdb;

	if (!strcmp(cmd, UPLOAD_CMD)) {
		int table;
		sscanf(argv[2], "%d", &table);
		const std::string prefix(argv[3]);
		ImageScanner *scanner = new YaleS3Scanner(prefix);
		rc = cvdb.upload(scanner, table, prefix);
	} else if (!strcmp(cmd, TRAIN_CMD)) {
		int table, start, stop, resolution;
		sscanf(argv[2], "%d", &table);
		sscanf(argv[3], "%d", &resolution);
		sscanf(argv[4], "%d", &start);
		sscanf(argv[5], "%d", &stop);
		std::pair<int, int> range(start, stop);
		rc = cvdb.train(table, resolution, range);
	} else if (!strcmp(cmd, LEARN_CMD)) {
		int table, start, stop;
		sscanf(argv[2], "%d", &table);
		sscanf(argv[3], "%d", &start);
		sscanf(argv[4], "%d", &stop);
		std::pair<int, int> range(start, stop);
		rc = cvdb.learn(table, range);
	} else if (!strcmp(cmd, QUERY_CMD)) {
		int table, image, start, stop;
		sscanf(argv[2], "%d", &table);
		sscanf(argv[3], "%d", &image);
		sscanf(argv[4], "%d", &start);
		sscanf(argv[5], "%d", &stop);
		std::pair<int, int> range(start, stop);
		rc = cvdb.query(table, image, range, std::cout);
	} else {
		rc = EXIT_FAILURE;
	}

	return rc;
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
