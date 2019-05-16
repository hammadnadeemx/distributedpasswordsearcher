package main

import "net"
import "fmt"
import "bufio"
import "os"
import "time"
import "strings"
import "sync"
import "strconv"

func searcher(word string, filename string) {// searches a file line by line for a match returns 1 if found and 2 otherwise
	var index=0
	mutex.Lock()
	var stop=int((totallines/totalnodes)*assignedpart)
	var start=int((totallines/totalnodes)*(assignedpart-1))
	mutex.Unlock()
	result="lost"
	f, err := os.Open(filename)
	if err != nil {
		fmt.Print("Error on reading file.\n")
	}
	defer f.Close()
	defer wg.Done()
	scanner := bufio.NewScanner(f)// Splits on newlines by default.
	L:
		for scanner.Scan() {
			select {
			case _ = <-signals:
				fmt.Println("Terminate signal.")
				break L // get out of the loop !!!!
			default:
				if index>=start{
					if strings.Contains(scanner.Text(), word) {
						result="found"
						break L  // get out of the loop !!!!
					}
				}
				if index>stop{
					break L					
				}
				index++
			}
		}
}

func returningfunc(c net.Conn){
	wg.Wait()// wait for search to complete or be interupted by server
	fmt.Println("Search completed result is "+result)
	result=result+"\n"
	c.Write([]byte(result))
}

var result string
var totallines=63941069 // total number of linesin file
var totalnodes=1 // divide by this number
var assignedpart=1 // denotes first ,second.. part of file to search
var mutex=&sync.Mutex{} // mutex for the top 2 variables
var signals=make(chan bool)
var wg sync.WaitGroup // semaphore used to controll execution of searcher thread. Multiple search orders from server not possible with current server implementation ie the server will never request it

func main(){
	fmt.Println("Connecting to server...") 
	var order string
	conn, err := net.Dial("tcp", "127.0.0.1:2000")
	if err != nil {
		fmt.Println("Error establishing connection.\n")
	}
	serverReader := bufio.NewReader(conn)
	for {
		fmt.Println("Attempting to read from socket.")
		command, _, err := serverReader.ReadLine()
		fmt.Println("Done reading.")
		if err==nil{
			order=string(command)
			if strings.Contains(order,"terminate"){// stop signal
				fmt.Println("Terminate signal.")// terminate signal
				select {
				case signals<-true:
					fmt.Println("Thread interupt sent.")
				default:
					fmt.Println("Thread finished execution before signal.")
				}	
			}else if strings.Contains(order,"ping-test-alive-con"){
				fmt.Println("Pinged from server.")
				conn.Write([]byte("slave-ok\n"))// used to check if connection is valid			
			}else if strings.Contains(order,"load-distribute"){
				fmt.Println("Load distribution reassigned by server.")
				s := strings.Split(order, "-")
				mutex.Lock()
				tn,_ := strconv.ParseInt(s[3], 10, 32)
				totalnodes=int(tn)
				fmt.Println(totalnodes)
				ap,_ := strconv.ParseInt(s[2], 10, 32)
				assignedpart=int(ap)
				fmt.Println(assignedpart)
				mutex.Unlock()			
			} else {
				fmt.Println("Initiating search for "+order)
				for len(signals) > 0 {// empty channel to prevent issues with massive concurrency !
					select {
					case <-signals:
						fmt.Println("Emptying channel.")
					default:
						fmt.Println("Nothing to empty from channel.")
					}
				}
				wg.Add(1)// to control the operation of below routine only
				go searcher(order,"passwordfile.txt")
				go returningfunc(conn)
			}
		} else{
			fmt.Println("Error on read.")
			time.Sleep(10 * time.Second)
		}	
	}
	conn.Close()
}
