#include "SugarDB.h"
/* Opening or Creating a database */
DB *db_open(const char *pathname, int oflag, int mode)
{
	DB *db;
	int i, len;
	char asciiptr[PTR_SZ + 1], hash[(NHASH_DEF + 1) * PTR_SZ +2]; /* +2 for newline and NULL */
	struct stat statbuff;
	/* Allocating a Database structure and the buffer it requires. */
	len = strlen(pathname);
	if((db = _db_alloc(len)) == NULL)
		err_dump("error");
	db->oflag = oflag; /* saving a copy of the open flags */
	/* Opening index file */
	strcpy(db->name, pathname);
	strcat(db->name, ".idx");
	if((db->idxfd = open(db->name, oflag, mode)) < 0)
	{
		_db_free(db);
		return(NULL);
	}
	/* Opening data file */
	strcpy(db->name + len, ".dat");
	if((db->datfd = open(db->name, oflag, mode)) < 0)
	{
		_db_free(db);
		return(NULL);
	}
	/* If the database was created, then initialize it */
	if((oflag & (O_CREAT | O_TRUNC)) == (O_CREAT | O_TRUNC))
	{
		/* write lock the entire file and so that, we can stat the file, check it's size & initialize it */
		if(writew_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("error");
		if(fstat(db->idxfd, &statbuff) < 0)
			err_sys("error");
		if(statbuff.st_size==0)
		{
			/* 
			We gotta build a list of (NHASH_DEF + 1) chain ptr with a value of 0.
			The +1 is for free list pointer that precedes the hash table.
			*/
			sprintf(asciiptr, "%*d", PTR_SZ, 0);
			hash[0] = 0;
			for(i=0;i<(NHASH_DEF + 1);i++)
				strcat(hash, asciiptr);
			strcat(hash, "\n");
			i = strlen(hash);
			if(write(db->idxfd, hash, i) != i)
				err_dump("error");
		}
		if(un_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("error");
	}
	db->nhash = NHASH_DEF; /* hash table size */
	db->hashoff = HASH_OFF; /* offset in index file of hash table */
							/* free list ptr always at FREE_OFF */
	db_rewind(db);
	return(db);
}

/* Allocating and Initializing a database structure and all the buffers it requires. */
DB *_db_alloc(int namelen)
{
	DB *db;
	/* use calloc initializing the structure to 0 */
	if((db = calloc(1, sizeof(DB))) == NULL)
		err_dump("error");
	db->idxfd = db->datfd = -1; /* descriptors */
	/* Allocation begins 
	+5 for ".idx" or ".dat" and also NULL at end.
	*/
	if((db->name = malloc(namelen + 5)) == NULL)
		err_dump("error");
	/* Allocating an index buffer and a data buffer
	+2 for newline and NULL at end.
	*/
	if((db->idxbuf = malloc(IDXLEN_MAX + 2)) == NULL)
		err_dump("error");
	if((db->datbuf = malloc(DATLEN_MAX + 2)) == NULL)
		err_dump("error");
	return(db);
}

/* Free-ing up the database structure and all the memory allocated (malloc) buffers which are pointed.
And close the file descriptors if it's still open */
int _db_free(DB *db)
{
	if(db->idxfd >= 0 && close(db->idxfd) < 0)
		err_dump("error");
	if(db->datfd >= 0 && close(db->datfd) < 0)
		err_dump("error");
	db->idxfd = db->datfd = -1;
	if(db->idxbuf != NULL)
		free(db->idxbuf);
	if(db->datbuf != NULL)
		free(db->datbuf);
	if(db->name != NULL)
		free(db->name);
	free(db);
	return(0);
}

/* Fetching a specified record.
We return a pointer to the null-terminated data. */
char *db_fetch(DB *db, const char *key)
{
	char *ptr;
	if(_db_find(db, key, 0) < 0)
	{
		ptr = NULL; /* error --- record not found --- */
		db->cnt_fetcherr++;
	}
	else
	{
		ptr = _db_readdat(db); /* returns pointers to data */
		db->cnt_fetchok++;
	}
	/* Unlock the hash chain that _db_find() locked */
	if(un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		err_dump("un_lock error");
	return(ptr);
}

/* Find specified record.
Called by db_delete(), db_fetch() and db_store() */
int _db_find(DB *db, const char *key, int writelock)
{
	off_t offset, nextoffset;
	/* calculate hash value for this key, then calculate byte offset of corresponding chain ptr in hash table.
	*/
	/* calculate offset in hash table for this key */
	db->chainoff = (_db_hash(db, key) * PTR_SZ) + db->hashoff;
	db->ptroff = db->chainoff;
	/* here's where we lock this hash chain. It's the caller responsibility to unlock it when done.
	Note that we lock and unlock only the first byte. */
	if(writelock)
	{
		if(writew_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("error");
	}
	else
	{
		if(readw_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("error");
	}
	/* Get the offset in the index file of first record on the bash chain (it can be 0 too) */
	offset = _db_readptr(db, db->ptroff);
	while(offset!=0)
	{
		nextoffset = _db_readidx(db, offset);
		if(strcmp(db->idxbuf, key) == 0)
			break; /* found a match */
		db->ptroff = offset; /* offset of this (unequal) record */
		offset = nextoffset; /* next one to compare */
	}
	if(offset == 0)
		return(-1); /* error -- record not found */
	return(0); /* if not error */
}

/* Calculating the hash value for the key */
hash_t _db_hash(DB *db, const char *key)
{
	hash_t hval;
	const char *ptr;
	char c;
	int i;
	hval = 0;
	for(ptr = key, i = 1; c = ptr++; i++)
		hval += c * i; /* ASCII char times it's l-based index */
	return(hval % db->nhash);
}

/* Read a chain ptr field from anywhere in the index file:
--- the free list pointer, hash table chain ptr, or index record chain ptr */
off_t _db_readptr(DB *db, off_t offset)
{
	char asciiptr[PTR_SZ + 1];
	if(lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("error");
	if(read(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("error");
	asciiptr[PTR_SZ] = 0; /* null terminate */
	return(atol(asciiptr));
}

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

/* Reading the current data record into the data buffer */
/* Returning the pointer to the null terminated data buffer */
char *_db_readdat(DB *db)
{
	if(lseek(db->datfd, db->datoff, SEEK_SET) == -1)
		err_dump("error");
	if(read(db->datfd, db->datbuf, db->datlen) != db->datlen)
		err_dump("error");
	if(db->datbuf[db->datlen - 1] != '\n') /* clean checking */
		err_dump("missing newline");
	db->datbuf[db->datlen - 1] = 0; /* replace newline with NULL */
	return(db->datbuf); /* returns pointer to data record */
}

/* Deleting the specified record */
int db_delete(DB *db, const char *key)
{
	int rc;
	if(_db_find(db, key, 1) == 0)
	{
		rc = _db_dodelete(db); /* record found */
		db->cnt_delok++;
	}
	else
	{
		rc = -1; /* not found */
		db->cnt_delerr++;
	}
	if(un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		err_dump("error");
	return(rc);
}

/* Deleting the current record specified by the database structure.
This function is called by db_delete() and db_store() after the record has been located by _db_find() */
int _db_dodelete(DB *db)
{
	int i;
	char *ptr;
	off_t freeptr, saveptr;
	/* Setting data buffer to all blanks */
	for(ptr = db->datbuf, i = 0; i < db->datlen - 1; i++)
		*ptr++ = ' ';
	*ptr = 0; /*null terminate for _db_writedat()*/
	/* Setting key to blanks */
	ptr = db->idxbuf;
	while(*ptr)
		*ptr++ = ' ';
	/* Locking the free list */
	if(writew_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	/* Writing the data record with all blanks */
	_db_writedat(db, db->datbuf, db->datoff, SEEK_SET);
	/* Reading the free list pointer.
	It's value becomes the chain ptr field of the deleted index record.
	This means that the deleted  record becomes the head of the free list. */
	freeptr = _db_readptr(db, FREE_OFF);
	/* Save the contents of the index record chain ptr before being re-written by _db_writeidx() */
	saveptr = db->ptrval;
	/* Re-writing the index record, which also re-writes the length of the index record , data offset & data length*/
	_db_writeidx(db, db->idxbuf, db->idxoff, SEEK_SET, freeptr);
	/* Writing the new free list pointer */
	_db_writeptr(db, FREE_OFF, db->idxoff);
	/* Re-writing the chain ptr that pointed to this record being deleted.
	Recalling that _db_find() sets db->ptroff to point to this chain ptr.
	We'll be setting this chain ptr to the contents of the deleted record's chain ptr, saveptr, which can be either zero or not */
	_db_writeptr(db, db->ptroff, saveptr);
	if(un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	return(0);
}

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

/* Writing a chain ptr field somewhere in the index file:
	=== The free list
	=== The hash table
	=== index record
*/
void _db_writeptr(DB *db, off_t offset, off_t ptrval)
{
	char asciiptr[PTR_SZ + 1];
	if(ptrval < 0 || ptrval > PTR_MAX)
		err_quit("invalid ptr: %d", ptrval);
	sprintf(asciiptr, "%*d", PTR_SZ, ptrval);
	if(lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("error");
	if(write(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("error");
}

/* Storing a record in a database.
Return 0 if okay, Return 1 if record exists and DB_INSERT specified.
Return -1 if record doesn't exist and DB_REPLACE specified.
*/
int db_store(DB *db, const char *key, const char *data, int flag)
{
	int rc, keylen, datlen;
	off_t ptrval;
	keylen = strlen(key);
	datlen = strlen(data) + 1; /* +1 is for newline at the end */
	if(datlen < DATLEN_MIN || datalen > DATLEN_MAX)
		err_dump("invalid data length");
	/* db_find() calculates which hash table this new record goes into(db->chainoff) whether it already exists or not.
	The calls to _db_writeptr() below change the hash table entry for this chain to point to new record.
	This means that the new record is added to the front of the hash chain */
	if(_db_find(db, key, 1) < 0)
	{
		/* record not found*/
		if(flag & DB_REPLACE)
		{
			rc = -1;
			db->cnt_storerr++;
			goto doreturn; /* error --- record doesn't exist */
		}
		/* do_find() locked the hash chain.
			Reading the chain ptr to the first index record on hash chain */
		ptrval = _db_readptr(db, db->chainoff);
		if(_db_findfree(db, keylen, datlen) < 0)
		{
			/* an empty record of the correct record wasn't found.
				we gotta append the new record to the ends of the index and data files */
			_db_writedat(db, data, 0, SEEK_END);
			_db_writeidx(db, key, 0, SEEK_END, ptrval);
			/* db->idxoff was set by _db_writeidx()
			The record goes to the front of the bash chain */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor1++;
		}
		else
		{
			/* Re-using an empty record
			_db_findtree() removed the record from the free list and set both db->datoff and db->idxoff */
			_db_writedat(db, data, 0, SEEK_END);
			_db_writeidx(db, key, 0, SEEK_END, ptrval);
			/* re-used record goes to the front of the hash chain */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor2++;
		}
	}
	else
	{
		/* record found */
		if(flag & DB_INSERT)
		{
			rc = 1;
			db->cnt_storerr++;
			goto doreturn; /* error --- record is already in the database */
		}
		/* We are replacing the existing record.
			We know the new key equals the existing key, but also we gotta check if the data records are the same size. */
		if(datlen != db->datlen)
		{
			_db_dodelete(db); /* delete the existing record */
			/* Re-read the chain ptr in the hash table (It may change with the deletion) */
			ptrval = _db_readptr(db, db->chainoff);
			/* Appending new index and data records to end of files */
			_db_writedat(db, data, 0, SEEK_END);
			_db_writeidx(db, key, 0, SEEK_END, ptrval);
			/* new record going to the front of the hash chain */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor3++;
		}
		else
		{
			/* Same sized data, just replaces data record */
			_db_writedat(db, data, db->datoff, SEEK_SET);
			db->cnt_stor4++;
		}
	}
	rc = 0; /* woo hoo */
doreturn: /* unlock the hash chain that _db_find() locked */
	if(un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		return(rc);
}

/* Try to find a free index record and accompany data record of correct sizes.
We're only called by db_store() */
int _db_findtree(DB *db, int keylen, int datalen)
{
	int rc;
	off_t offset, nextoffset, saveoffset;
	/* locking the free list */
	if(writew_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	/* reading the free list pointer */
	saveoffset = FREE_OFF;
	offset = _db_readptr(db, saveoffset);
	while(offset != 0)
	{
		nextoffset = _db_readidx(db, offset);
		if(strlen(db->idxbuf) == keylen && db->datlen == datlen)
			break; /* found a match */
		saveoffset = offset;
		offset = nextoffset;
	}
	if(offset == 0)
		rc = -1; /* no match found */
	else
	{
		/* Found a tree record with matching sizes.
			The index record was read in by _db_readidx() above which sets db->ptrval.
			Also, saveoffset points to the chain ptr that pointed to empty record on free list.
			We'll be setting this chain ptr to db->ptrval, which removes empty record from free list
			*/
		_db_writeptr(db, saveoffset, db->ptrval);
		rc = 0;
		/* _db_readidx() set both db->idxoff and db->datoff.
			This is used by the caller, db_store() inorder to write new index record and data record
		*/
	}
	/* Unlocking the free list */
	if(un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	return(rc);
}

/* Rewinding the index file for db_nextrec()
Then, automatically called by db_open()
And it must be called before db_nextrec() --- oops! */
void db_rewind(DB *db)
{
	off_t offset;
	offset = (db->nhash + 1) * PTR_SZ; /* +1 for free list pointer */
	/* Then, we are just setting the file offset for this process to the start of the index records,
		Also, not required to lock.
		+1 for newline at the end of hash table
	*/
	if((db->idxoff = lseek(db->idxfd, offset+1, SEEK_SET)) == -1)
		err_dump("error");
}

/* Returning the sequential record...
Just gotta step ahead through the index file where we gotta ignore deleted records.
db_rewind() essential to be called before this function at initial stage itself */
char *db_nextrec(DB *db, char *key)
{
	char c, *ptr;
	/* Locking the free list where we don't actually read a record in the mid of deletion*/
	if(readw_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	do
	{
		/* read next sequential index record */
		if(_db_readidx(db, 0) < 0)
		{
			ptr = NULL; /* end of index file --- EOF */
			goto doreturn;
		}
		/* Checking if the key is still blank or empty record */
		ptr = db->idxbuf;
		while((c = *ptr++) != 0 && c = ' ');
		/* skip if it's not blank */
	}
	while(c == 0) /* loop untill a non-empty key is found*/
		if(key != NULL)
			strcpy(key, db->idxbuf); /* return key */
	ptr = _db_readdat(db); /* return pointer to data buffer */
	db->cnt_nextrec++;
doreturn:
	if(un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
}

void db_close(DB *db)
{
	_db_free(db); /* closes file descriptors, free the buffers & struct */
}
