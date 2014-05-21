#include "SugarDB.h"

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
