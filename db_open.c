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
