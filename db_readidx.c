#include "SugarDB.h"
#include<sys/uio.h> /* struct iovec */
/* Reading next index record.
Starting at the specified offset in the index file.
Reading the index record into db->idxbuf and replacing the separators with null bytes.
If all were cool, then setting db->datoff and db->datlen to the offset & length of the corresponding data record in data file.
*/
off_t _db_readidx(DB *db, off_t offset)
{
	int i;
	char *ptr1, *ptr2;
	char asciiptr[PTR_SZ + 1], asciilen[IDXLEN_SZ + 1];
	struct iovec iov[2];
	/* Positioning index file and recording the offset.
	db_nextrec() calls with offset==0 which means reading from current offset.
	we still need to call lseek() to record the current offset. */
	if((db->idxoff = lseek(db->idxfd, offset, offset == 0 ? SEEK_CUR : SEEK_SET)) == -1)
		err_dump("error");
	/* Reading the ASCII chain ptr and ASCII length at the front of index record.
	This tells us the remaining size of the index record. */
	iov[0].iov_base = asciiptr;
	iov[0].iov_len = PTR_SZ;
	iov[1].iov_base = asciilen;
	iov[1].iov_len = IDXLEN_SZ;
	if((i = readv(db->idxfd, &iov[0], 2)) != PTR_SZ + IDXLEN_SZ)
	{
		if(i == 0 && offset == 0)
			return(-1); /* EOF for db_nextrec() */
	err_dump("error");
	}
asciiptr[PTR_SZ] = 0; /* null terminate */
db->ptrval = atol(asciiptr); /* offset of next key in chain */
							/* this is our return value, always >= 0 */
asciilen[IDXLEN_SZ] = 0; /* null terminate */
if((db->idxlen = atoi(asciilen)) < IDXLEN_MIN || db->idxlen > IDXLEN_MAX)
	err_dump("invalid length");
/* Now reading the actual index record. We'll be reading it into the buffer we have memory allocated after we opened a DB */
if((i = read(db->idxfd, db->idxbuf, db->idxlen)) != db->idxlen)
	err_dump("error");
if(db->idxbuf[db->idxlen-1] != '\n')
	err_dump("missing newline"); /* clean checking */
db->idxbuf[db->idxlen-1] = 0; /* replace newline with NULL */
/* Finding the separators in the index record */
if((ptr1 = strchr(db->idxbuf, SEP)) == NULL)
	err_dump("missing first separator");
*ptr1++ = 0;
if((ptr2 = strchr(ptr1, SEP)) == NULL)
	err_dump("missing seconf separator");
*ptr2++ = 0;
if(strchr(ptr2, SEP) != NULL)
	err_dump("too many separators");
/* Getting the initial offset and length of the data record */
if((db->datoff = atol(ptr1)) < 0)
	err_dump("initial offset < 0");
if((db->datlen = atol(ptr2)) <= 0 || db->datlen > DATLEN_MAX)
	err_dump("invalid length");
return(db->ptrval); /* return the offset of the next key in chain */
}

