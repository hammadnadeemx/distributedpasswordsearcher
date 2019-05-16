package main

import "net"
import "fmt"
import "bufio"
import "sync"// for mutex lock
import "time"
import "log"
import "strings"
import "strconv"

func clientthread(c net.Conn){// every client request has its own thread
	clientReader := bufio.NewReader(c)
	L:
		for{
			pswd, _,error2 := clientReader.ReadLine()// do error handling
			if error2!=nil{
				println("Connection dropped.")
				break L
			}
			if len(pswd)>1{
				fmt.Println("From client: "+string(pswd)) 
				cjmmutex.Lock()
				var lstcheck int =completedjobs[string(pswd)]	
				cjmmutex.Unlock()
				if lstcheck==0{// password has not been searched before so add it to joblist
					jlmutex.Lock()
					joblist=append(joblist,string(pswd))
					jlmutex.Unlock()
				}
				for {// create logic to check periodically if clients job is complete
					time.Sleep(1 * time.Second)
					cjmmutex.Lock()
					var condition int = completedjobs[string(pswd)]
					cjmmutex.Unlock()
					if condition==0 {
						fmt.Println("waiting on slaves for response.") 
					} else if condition==1	{
						fmt.Println("Password found.")
						_,error2:=c.Write([]byte("password found.\n"))
						if error2!=nil{
							println("Connection dropped.")
							break L
						}
						break
					} else {
						fmt.Println("Not found.")
						_,error3:=c.Write([]byte("password not found.\n"))
						if error3!=nil{
							println("Connection dropped.")
							break L
						}
						break
					}
				}
			} else {
				fmt.Println("Empty reads from tcp conn.")
				time.Sleep(1 * time.Second)
			}
		}
	c.Close()// close connection
	wg.Done()// at the end of the thread to remove count of wait group
}

func slavethread(c net.Conn){// add slave to list and update number
	noasmutex.Lock()
	numofactiveslaves=numofactiveslaves+1
	noasmutex.Unlock()
	slmutex.Lock()
	slavelist=append(slavelist,c)
	slmutex.Unlock()
	fmt.Println("Slave successfully registered.")
	wg.Done()// at the end of the thread to remove count of wait group
}
	
func clienthandle(){// this thread will deal with clients
	ln, err := net.Listen("tcp", ":3000") //clients connect to port 3000
	if err != nil {
		log.Fatal(err)
		fmt.Println("Port already in use.")  // listen on all interfaces
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		wg.Add(1)
		go clientthread(conn)
	}
	wg.Done()
}
func slavehandle(){// this thread deals with the work horses
	ln, err := net.Listen("tcp", ":2000") //slaves connect to port 2000
	if err != nil {
		log.Fatal(err)
		fmt.Println("Port already in use.")
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		wg.Add(1)
		go slavethread(conn)
	}
	wg.Done()
}

func orderslave(word string){// map slave code to this function
	slmutex.Lock()
	currlist:=append(slavelist[:0:0], slavelist...)// remove : and try
	slmutex.Unlock()
	job:=false
	var rcount=0// used to terminate slaves reply count
	var lcount=0 // lost count
	var maxcons=len(currlist)
	for _, element := range currlist {// tell all slaves to search for word
		element.Write([]byte(word+"\n"))		
	}// wait for response if found stop all other slaves
	for rcount<maxcons{// number of replied conn less than max connections
		for _, element := range currlist {
			if job==false{// check each conn for messages
				slaveReader := bufio.NewReader(element)
				message, _,err := slaveReader.ReadLine()//i am assuming this is non blocking
				//fmt.Println("Response recieved from slave is "+string(message))
				if err == nil{ // it means it got some input from the slaves
					if strings.Contains(string(message), "fou"){// found
						fmt.Println("Password found.")
						cjmmutex.Lock()
						completedjobs[word]=1
						cjmmutex.Unlock()
						rcount++
						job=true
						// add result to map ( dont need to return to client its done in the client thread)
					} else if strings.Contains(string(message), "los"){// lost 
						lcount++
						if lcount>=maxcons{// all nodes cant find the password
							fmt.Println("Password not found by any slave.")
							cjmmutex.Lock()
							completedjobs[word]=2
							cjmmutex.Unlock()
							job=true
						}						
					}				
				}else{
					fmt.Println("slave dropped out.")
					rcount++					
				}
			} else{ // notify each conn to stop
				fmt.Println("Terminating slaves.")
				element.Write([]byte("terminate\n"))		
				rcount++		
			}
		}
		//sleepstuff
		fmt.Println("Server is waiting for nodes to complete processing.")
		time.Sleep(10 * time.Second)
	}	
}

func jobhandle(){
	for{
		var word string
		jlmutex.Lock()
		if len(joblist)>0 {// check list for ready jobs
			word=joblist[0]
			jlmutex.Unlock()			
			// check connections for inactivity and purge as required <----- important step might need to add code to cater for slaves droping out during computations
			noasmutex.Lock()
			slmutex.Lock()
			var newlist [] net.Conn
			for _, element := range slavelist {// tell all slaves to search for word
				_,erort := element.Write([]byte("ping-test-alive-con\n"))
				if erort == nil	{
					testreader := bufio.NewReader(element)
					reply,_,_:=testreader.ReadLine()
					if strings.Contains(string(reply),"slave-ok"){
						newlist=append(newlist,element) // responsive so add to list
					}
				}// dont add it to list
			}
			numofactiveslaves=len(newlist)
			slavelist=newlist// replace with latest one
			slmutex.Unlock()			
			currentslaves:=numofactiveslaves// send slaves order to work
			var part=1
			for _, element := range newlist {// tell all slaves to adjust distribution
				var tmpstr="load-distribute-"
				tmpstr+=strconv.Itoa(part)+"-"+strconv.Itoa(numofactiveslaves)
				part++
				fmt.Println(tmpstr)
				_,_ = element.Write([]byte(tmpstr+"\n"))
			}
			noasmutex.Unlock()
			if currentslaves>0 {// work other wise wait
				orderslave(word)// returns when search is completed
				jlmutex.Lock()// remove word from job list
				copy(joblist[0:],joblist[1:]) // Shift a[i+1:] left one index.
				joblist[len(joblist)-1] = ""     // Erase last element (write zero value).
				joblist = joblist[:len(joblist)-1]	
				jlmutex.Unlock()		
			} else {
				fmt.Println("Server has no registered slaves.")
				time.Sleep(10 * time.Second)
			}
		} else{//idling
			jlmutex.Unlock()
			fmt.Println("Server is idle.")
			time.Sleep(1 * time.Second)			
		}
	}
	wg.Done()
}

var numofactiveslaves=0// number of connected active slaves
var noasmutex=&sync.Mutex{} // mutex for above variable
var slmutex=&sync.Mutex{}// mutex for variable below
var slavelist [] net.Conn// list of registered slaves need to check and clean before the start of each job to remove zombies
var wg sync.WaitGroup // semaphore for preventing main from exiting early
var jlmutex=&sync.Mutex{}//mutex for joblist 
var cjmmutex=&sync.Mutex{} // mutex for completedjobs map
var joblist [] string // list of passwords requested by clients
var completedjobs=map[string]int{} // password and 1 for found 2 for not found 0 for key doesnt exist <-- map properties in go

func main() {
	fmt.Println("Launching server...")  // listen on all interfaces
	wg.Add(1)
	go clienthandle()
	wg.Add(1)
	go slavehandle()
	wg.Add(1)
	go jobhandle()
	wg.Wait() // waits for all threads to complete. below this is unreachable code !
	fmt.Print("Server shuting down.\n")
}
