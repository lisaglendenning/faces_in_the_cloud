/****************************************************************************
 ****************************************************************************/

#include "yale.h"

#include <cstdio>


///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

/* root level info file */
static const std::string INFO = "yaleB.info";

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

YaleS3Scanner::YaleS3Scanner(const std::string& prefix)
  : s3prefix(prefix), sid(-1), pid(-1) { }

///////////////////////////////////////////////////////////////////////////////

void
YaleS3Scanner::open()
{
	// open top level info
	std::string key(s3prefix);
	key += "/" + INFO;
	root = s3connect()->get(CVDB::BUCKET, key);
}

///////////////////////////////////////////////////////////////////////////////

bool
YaleS3Scanner::next(ImageMetadata &meta)
{
	GetResponsePtr res;
	std::string filename;

	// Get next info file
	while (1) {
		if (sid == -1) {
			if (root->getInputStream().eof()) {
				return false;
			}
			root->getInputStream() >> filename;
			if (filename.size() == 0) {
				return false;
			}

			size_t index = filename.find_last_of('/');
			assert(index >= 0);
			cwd.assign(filename.substr(0, index));
			std::string suffix = filename.substr(index + 1);
			sscanf(suffix.c_str(), "yaleB%02d_P%02d.info", &sid, &pid);
			std::string key(s3prefix);
			key += "/" + filename;
			info = s3connect()->get(CVDB::BUCKET, key);

			// ignore background image
			info->getInputStream() >> filename;
		}

		// Get next image from info file
		if (!info->getInputStream().eof()) {
			info->getInputStream() >> filename;
			if (filename.size() != 0) {
				// skip any nonexisting files
				std::string key(s3prefix);
				key += "/";
				key += cwd;
				key += "/";
				key += filename;
				try {
					res = s3connect()->get(CVDB::BUCKET, key);
				} catch (GetException &e) {
					continue;
				}

				break;
			}
		}
		sid = -1;
		pid = -1;
	}

	meta.name += cwd + "/" + filename;
	meta.subjectid = sid;
	meta.poseid = pid;

	// read image header for remaining metadata
	read_header(res->getInputStream(), meta.dimensions, meta.format);

	return true;
}

///////////////////////////////////////////////////////////////////////////////

void
YaleS3Scanner::close()
{
  sid = -1;
  pid = -1;
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
