/****************************************************************************
 *
 * Defines an ImageScanner that extracts the image metadata in a
 * Yale format database in S3.
 *
 ****************************************************************************/

#ifndef CLOUDVISION_YALE_H
#define CLOUDVISION_YALE_H


#include "image.h"
#include "aws.h"

#include <string>
#include <vector>
#include <iostream>


///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

class YaleS3Scanner: public ImageScanner
{
public:
	YaleS3Scanner(const std::string& prefix);

	void open();
	bool next(ImageMetadata& meta);
	void close();

private:
	std::string s3prefix;
	std::string cwd;
	GetResponsePtr root;
	GetResponsePtr info;
	int sid;
	int pid;
};

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

#endif // CLOUDVISION_YALE_H
