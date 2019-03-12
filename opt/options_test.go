package opt

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"testing"
)

func TestSaveConfig(t *testing.T) {
	file, err := ioutil.TempFile("", "sample-config.json")
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultOptions()
	SaveConfig(config, file.Name())
	t.Log("Save config file success", file.Name())
}

func TestFinalizeConfig(t *testing.T) {
	config := DefaultOptions()
	config.TotalVolumn = 128 << 40
	config.IndexTableCols = 1024
	config, err := FinalizeConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	str, err := json.MarshalIndent(config, "", "	")
	fmt.Println(string(str))
}

func TestParseConfig(t *testing.T) {
	config := DefaultOptions()
	config.TotalVolumn = 128 << 40
	config.IndexTableCols = 1024
	file, err := ioutil.TempFile("", "sample-config.json")
	if err != nil {
		t.Fatal(err)
	}
	SaveConfig(config, file.Name())

	str, err := json.MarshalIndent(config, "", "	")
	fmt.Println(string(str))

	configNew, err := ParseConfig(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	str, err = json.MarshalIndent(configNew, "", "	")
	fmt.Println(string(str))
}
