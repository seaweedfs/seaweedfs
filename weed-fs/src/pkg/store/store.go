package store

import (
    "log"
    "strconv"
)
type Store struct{
  volumes []*Volume
  dir string
  
  freeVolumeChannel chan int
}
func NewStore(dirname string, count int) (s *Store){
  s = new(Store)
  s.dir = dirname
  s.volumes = make([]*Volume,count)
  s.freeVolumeChannel = make(chan int, count)
  for i:=0;i<count;i++{
    s.volumes[i] = NewVolume(s.dir, strconv.Itob(i,16))
    s.freeVolumeChannel <- i
  }
  log.Println("Store started on dir:", dirname, "with", count,"volumes");
  return
}
func (s *Store)Close(){
  close(s.freeVolumeChannel)
  for _, v := range s.volumes{
    v.Close()
  }
}
func (s *Store)Write(n *Needle)(int){
  i := <- s.freeVolumeChannel
  s.volumes[i].write(n)
  s.freeVolumeChannel <- i
  return i
}
func (s *Store)Read(i int, n *Needle){
  s.volumes[i].read(n)
}
