package logfile

import "os"

type FileLock struct {
	//uintptr
	//mu sync.RWMutex
	fd os.File
}

// 写锁
func (l *FileLock) Lock() error {
	/*
		    f, err := os.Open(l.dir)
			if err != nil {
				return err
			}
			l.f = f
			err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err != nil {
				return fmt.Errorf("cannot flock directory %s - %s", l.dir, err)
			}
			return nil
	*/
	return nil
}

// 读锁
func (l *FileLock) RLock() error {
	/*
		    f, err := os.Open(l.dir)
			if err != nil {
				return err
			}
			l.f = f
			err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err != nil {
				return fmt.Errorf("cannot flock directory %s - %s", l.dir, err)
			}
			return nil
	*/
	return nil
}

//释放锁
func (l *FileLock) Unlock() error {
	/*
		    defer l.f.Close()
			return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	*/
	return nil
}
