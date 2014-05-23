#include "SugarDB.h"
#include<sys/uio.h> /*struct iovec */
/* writing an index record
_db_writedat() is called before this function inorder to set fields datoff and datlen in the database structure where we gotta write index record */
void _db_writeidx(DB *db, const char *key, off_t offset, int whence, off_t ptrval)
{
	struct iovec iov[2];
	char asciiptrlen[PTR_SZ + IDXLEN_SZ + 1];
	int len;
	if((db->ptrval = ptrval) < 0 || ptrval > PTR_MAX)
		err_quit("invalid ptr: %d", ptrval);
	sprintf(db->idxbuf, "%s%c%d%c%d\n", key, SEP, db->datoff, SEP, db->datlen);
	if((len = strlen(sb->idxbuf)) < IDXLEN_MIN || len > IDXLEN_MAX)
		err_dump("invalid length");
	sprintf(asciiptrlen, "%*d%*d", PTR_SZ, ptrval, IDXLEN_SZ, len);
	/* If we are appending, then we gotta lock before doing lseek() and write() in making the two as atomic operation.
	If we are overwriting an existing record, then we don't have to lock */
	if(whence == SEEK_END) /* we are appending, then lock the entire file. */
		if(writew_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("error");
	if((db->idxoff = lseek(db->idxfd, offset, whence)) == -1)
		err_dump("error");
	iov[0].iov_base = asciiptrlen;
	iov[0].iov_len = PTR_SZ + IDXLEN_SZ;
	iov[1].iov_base = db->idxbuf;
	iov[1].iov_len = len;
	if(writev(db->idxfd, &iov[0], 2) != PTR_SZ + IDXLEN_SZ + len)
		err_dump("error");
	if(whence == SEEK_END)
		if(un_lock(db->idxfd, ((db->nhash + 1)*PTR_SZ)+1, SEEK_SET, 0) < 0)
			err_dump("error");
}
