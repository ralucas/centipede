//go:build unit || load

package fixtures

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/ralucas/centipede/internal/schema"
)

type TestFixture struct {
	data []byte
}

func NewTestFixture() *TestFixture {
	return &TestFixture{}
}

func (f *TestFixture) DatasetFilePath(name string) (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get filepath")
	}
	return path.Join(path.Dir(filename), "..", fmt.Sprintf("testdata/%s", name)), nil
}

func (f *TestFixture) DatasetMaps() ([]map[string]interface{}, error) {
	p, err := f.DatasetFilePath("dataset_array.json")
	if err != nil {
		return nil, err
	}

	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}

	var datasets []map[string]interface{}

	err = json.Unmarshal(b, &datasets)
	if err != nil {
		return nil, err
	}

	return datasets, nil
}

func (f *TestFixture) SingleRawDataset() ([]byte, error) {
	p, err := f.DatasetFilePath("dataset_single.json")
	if err != nil {
		return nil, err
	}
	return os.ReadFile(p)
}

func (f *TestFixture) BuildHugeDatasetFile() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to get filepath")
	}
	filepath := path.Join(path.Dir(filename), "..", "testdata/huge_dataset_array.json")

	info, err := os.Stat(filepath)
	if info != nil {
		return filepath, nil
	}

	var datasets [3000000]*schema.DatasetJson

	for i := 0; i < 3000000; i++ {
		dt := schema.DatasetJsonTypeDcatDataset
		vct := schema.VcardJsonTypeVcardContact
		ot := schema.OrganizationJsonTypeOrgOrganization

		size := gofakeit.IntRange(2, 10)
		keywords := make([]string, size)
		for j := 0; j < size; j++ {
			keywords[j] = gofakeit.Adjective()
		}

		ds := &schema.DatasetJson{
			Type:               &dt,
			AccessLevel:        "",
			AccrualPeriodicity: nil,
			BureauCode:         []string{},
			ConformsTo:         nil,
			ContactPoint: schema.VcardJson{
				Type:     &vct,
				Fn:       gofakeit.Name(),
				HasEmail: "",
			},
			DataQuality:            nil,
			DescribedBy:            nil,
			DescribedByType:        nil,
			Description:            gofakeit.Paragraph(1, 2, 30, ". "),
			Distribution:           nil,
			Identifier:             "GSA-2021-03-30-03",
			IsPartOf:               nil,
			Issued:                 nil,
			Keyword:                keywords,
			LandingPage:            nil,
			Language:               "en-US",
			License:                nil,
			Modified:               gofakeit.PastDate().String(),
			PrimaryITInvestmentUII: nil,
			ProgramCode:            []string{},
			Publisher: schema.OrganizationJson{
				Type: &ot,
				Name: gofakeit.Company(),
				SubOrganizationOf: &schema.OrganizationJson{
					Type:              &ot,
					Name:              gofakeit.Company(),
					SubOrganizationOf: &schema.OrganizationJson{},
				},
			},
			References:      nil,
			Rights:          nil,
			Spatial:         nil,
			SystemOfRecords: nil,
			Temporal:        nil,
			Theme:           nil,
			Title:           gofakeit.Phrase(),
		}

		datasets[i] = ds
	}

	file, err := os.Create(filepath)
	defer file.Close()
	if err != nil {
		return "", err
	}

	err = json.NewEncoder(file).Encode(datasets)
	if err != nil {
		return "", err
	}

	fmt.Println("created huge_dataset_array.json file")

	return filepath, nil
}
