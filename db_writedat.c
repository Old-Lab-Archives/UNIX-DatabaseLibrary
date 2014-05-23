#include "SugarDB.h"
#include<sys/uio.h> /* struct iovec */
/* Writing a data record and called by _db_dodelete() and db_store() */
void _db_writedat(DB *db, const char *data, off_t offset, int whence)
{
	struct iovec iov[2];
	static char newline = '\n';
	/* If we are appending, then we gotta lock before doing lseek() and write() in making the two as atomic operation.
	If we are overwriting an existing record, then we don't have to lock */
	if(whence == SEEK_END) /* we are appending, then lock the entire file. */
		if(writew_lock(db->datfd, 0, SEEK_SET, 0) < 0)
			err_dump("error");
	if((db->datoff = lseek(db->datfd, offset, whence)) == -1)
		err_dump("error");
	db->datlen = strlen(data) + 1; /* datlen includes newline */
	iov[0].iov_base = (char *) data;
	iov[0].iov_len = db->datlen - 1;
	iov[1].iov_base = &newline;
	iov[1].iov_len = 1;
	if(writev(db->datfd, &iov[0], 2) != db->datlen)
		err_dump("error");
	if(whence == SEEK_END)
		if(un_lock(db->datfd, 0, SEEK_SET, 0) < 0)
			err_dump("error");
}
