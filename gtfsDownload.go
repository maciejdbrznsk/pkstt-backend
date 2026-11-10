package main

/*
import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

func main() {
	url := "https://kmrzeszow.live/otwartedane/gtfsrt/pks/vehicle-positions.pb"
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error: Download failed: %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error: Reading data failed: %v", err)
	}

	feed := &gtfs.FeedMessage{}
	if err := proto.Unmarshal(data, feed); err != nil {
		log.Fatalf("Error: Decoding data failed: %v", err)
	}

	fmt.Printf("Received %d objects: \n", len(feed.Entity))
	for _, entity := range feed.Entity {
		if v := entity.GetVehicle(); v != nil {
			pos := v.GetPosition()
			fmt.Printf("Vehicle ID; %-8s | Trip: %-12s | GPS: (%.4f, %.4f)\n",
				v.GetVehicle().GetId(),
				v.GetTrip().GetTripId(),
				pos.GetLatitude(),
				pos.GetLongitude(),
			)
		}
	}
}

*/
