#ifndef __juice_h
#define __juice_h
#include<sys/types.h> /* required for some of our prototypes */
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#define MAXLINE 4096 /* maximum line length */
#define FILE_MODE(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) /* default file access permissions for new files */
#define DIR_MODE(FILE_MODE | S_IXUSR | S_IXGRP | S_IXOTH) /* default permissions for new directories */
typedef void Sigfunc(int); /* for singal handlers */
#if defined(SIG_IGN) && !defined(SIG_ERR)
#define SIG_ERR((Sigfunc *) -1)
#endif
#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))
/* Function prototypes */
char *path_alloc(int *);
int open_max(void);
void clr_fl(int, int);
void set_fl(int, int);
void pr_exit(int);
void pr_mask(const char*);
Sigfunc *signal_intr(int, Sigfunc *);
int tty_cbreak(int);
int tty_raw(int);
int tty_reset(int);
void tty_atexit(void);
#ifdef ECHO /* only if <terminals.h> is included */
struct termios *tty_termios(void);
#endif
void sleep_us(unsigned int);
ssize_t readn(int, void *, size_t);
ssize_t writen(int, const void *, size_t);
int daemon_init(void);
int s_pipe(int *);
int recv_fd(int, ssize_t (*func) (int, const void *, size_t));
int send_fd(int, int);
int send_err(int, int, const char *);
int serv_listen(const char *);
int serv_accept(int, uid_t *);
int cli_conn(const char *);
int buf_args(char *, int (*func) (int, char **));
int ptym_open(char *);
int ptys_open(int, char *);
#ifdef TIOCGWINSZ
pid_t pty_fork(int *, char *, const struct termios *, const struct winsize *);
#endif
int lock_reg(int, int, int, off_t, int, off_t);
#define read_lock(fd, offset, whence, len) \ lock_reg(fd, F_SETLK, F_RDLCK, offset, whence, len)
#define readw_lock(fd, offset, whence, len) \ lock_reg(fd, F_SETLKW, F_RDLCK, offset, whence, len)
#define write_lock(fd, offset, whence, len) \ lock_reg(fd, F_SETLK, F_WRLCK, offset, whence, len)
#define writew_lock(fd, offset, whence, len) \ lock_reg(fd, F_SETLKW, F_WRLCK, offset, whence, len)
#define un_lock(fd, offset, whence, len) \ lock_reg(fd, F_SETLK, F_UNLCK, offset, whence, len)
pid_t lock_test(int, int, off_t, int off_t);
#define is_readlock(fd, offset, whence, len) \ lock_test(fd, F_RDLCK, offset, whence, len)
#define is_writelock(fd, offset, whence, len) \ lock_test(fd, F_WRLCK, offset, whence, len)
void err_dump(const char *, ...);
void err_msg(const char *, ...);
void err_quit(const char *, ...);
void err_ret(const char *, ...);
void err_sys(const char *, ...);
void log_msg(const char *, ...);
void log_open(const char *, ...);
void log_quit(const char *, ...);
void log_ret(const char *, ...);
void log_sys(const char *, ...);
void TELL_WAIT(void);
void TELL_PARENT(pid_t);
void TELL_CHILD(pid_t);
void WAIT_PARENT(void);
void WAIT_CHILD(void);
#endif

/* NOTE: the file might be re-updated soon */
