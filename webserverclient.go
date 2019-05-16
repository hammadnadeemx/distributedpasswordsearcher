package main

import "html/template"
import "net/http"
import "net"
import "fmt"
import "bufio"

type pair struct {
	Success bool
	Pass string
}

func servcommunicator(conn net.Conn, paswd string)(string){// need to parse from website and remove loop
	serverReader := bufio.NewReader(conn)
	fmt.Fprintf(conn, paswd + "\n")
	response, _ , error1:= serverReader.ReadLine()
	if error1!=nil{
		return "Server was out of reach."
	}
	return string(response)
}

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:3000")
	if err != nil {
		//log.Fatal(err)
		fmt.Println("Error establishing connection.\n")
	} else{
		paspair:=pair{false,"pass"}
		tmpl := template.Must(template.ParseFiles("forms.html"))
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				tmpl.Execute(w, nil)
				return
			}
			password :=r.FormValue("pswd")
			paspair.Success=true
			paspair.Pass=servcommunicator(conn,password)
			fmt.Println("Request is "+password)
			tmpl.Execute(w,paspair)
		})
		http.ListenAndServe(":8080", nil)
	}
	conn.Close()
}
