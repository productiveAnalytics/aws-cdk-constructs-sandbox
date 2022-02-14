#! /usr/bin/env bash

UUID=(
"17dadfab-c44d-461b-8c24-ff5468e5e0a3"
"29eb8a27-eec4-4b96-8fd7-0e4e64c85b98"
"6dc8ba8d-90e3-4e7a-ad95-dde28df1c79c"
"0a8b44e4-0710-4384-8a70-565b80d36856"
"72af9abd-8f55-4699-9835-69f2d7735d39"
"33a17fc7-922d-4368-b154-54544b09a52c"
"092259c9-1894-4223-9285-66622fccde14"
"a38a5322-6939-4e9f-ab91-1f33df6eb4e7"
"b8999b10-3650-4021-9be9-fb82e09e53ff"
"072eeb12-3ab5-45f6-9e7a-19996966b642"
"cfeaab38-a9f9-420d-abcb-b1e4c9c24099"
"5c92848b-4b05-4999-9f39-6771e687a383"
"262fa57b-af23-4835-bb39-4aa28b04fc97"
"12e26f4c-c360-4d3e-aa37-6e6cbe0d2409"
"b1b846af-af0f-4ef6-8c37-9b74b1a2054c"
"7ede4d86-60ff-4e63-ba35-f137521feb07"
"9790cc7d-d153-45e5-b9cb-b68b88264eba"
"0a5866b6-40db-49f0-a3e8-bef776914244"
"02b85a5b-6157-4515-a04b-534e56d1d256"
"52fb8be3-4957-44d2-8848-a3e9e53f9480"
"d5d18b38-6681-466b-87fb-c48c4fa9dd19"
"4807d285-57f4-4aae-aef6-9d1428cc3ff9"
"fb6fe74f-433e-4370-9bc9-be7c76c83761"
"1ff133f6-7e3b-44a5-ab67-0ab0a2d76370"
"2ae55408-686a-4bdf-abda-7708ad7d722e"
"fc5893dc-debe-4e2c-8959-1cc34b71d930"
"1e6cdda2-215c-496b-aec0-1cbc562033bd"
"28d437d4-c583-474c-8a20-d07ceebd8e53"
"f5a4bdc6-b162-4e8d-bea5-85d3ac93e7b8"
"98d0b144-6ca8-495a-8452-df405ceafbd4"
"0e6b4bbf-d962-4e99-a272-699a9c3bbb30"
"e6c16d09-1298-4981-8ce4-10daac3c5a42"
"e88312ff-9a73-469b-b6a9-72d98955401f"
"fb86f43c-eed9-45f3-a263-84dc8f2283b3"
"3b10fa23-26c6-4d7a-86db-23b58ab1c669"
"f6aeaea4-2ee6-42c1-a310-421408149449"
"83750124-39b8-4786-af92-cc92e9b2bfae"
"a45957e2-dac0-412b-9a55-f222073abf5f"
"26e2a8d0-b5ed-4f3a-ad52-2c2d4ed671ba"
"e1fd1edf-88cd-4791-8d41-4bafdc2f7e58"
"146ce47c-7d8b-4b67-9bca-bc772a4fa30b"
"995744fb-25f8-4e79-9a9e-c179c5eeca08"
"83da8da1-4a9a-404d-9808-6e7e1b880f80"
"8ce4deae-bc4f-4090-91e5-a297b46d6489"
"eff0240e-0efe-432d-abad-0ce4f7867cbf"
"2001418d-f873-42d3-9b85-049e26000e9d"
"69009150-3209-47d9-9af7-a012073b879b"
"2a480a21-1185-40cc-98a0-43d547c10d74"
"c9459a9c-9c12-44ba-a3b0-5c8bc5a0ccda"
"40a0d52b-dcf3-49ab-bc9a-4f76496bfd75"
"48376efd-4e2e-487b-a830-e1d0b073c631"
"21f9d342-84b6-4a94-b2bf-b0c2fcf6da6b"
"ccc60aa8-ad38-4acb-985b-93f529c0bc6e"
)

for lambda_trigger_uuid in "${UUID[@]}"
do
  echo "trigger : ${lambda_trigger_uuid}"
  aws lambda update-event-source-mapping --uuid ${lambda_trigger_uuid} --enable
done