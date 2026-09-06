import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { SavedNews } from "../../../app/components/saved-news";
const id = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e781";
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it("keeps absent and empty news distinct and performs only saved reads", async () => {
  const fetcher = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({data:{instrument_id:id,status:"unavailable"}})))
    .mockResolvedValueOnce(new Response(JSON.stringify({data:{instrument_id:id,status:"empty",articles:[],has_more:false}})));
  vi.stubGlobal("fetch", fetcher);
  render(<SavedNews instrumentId={id} />);
  expect(await screen.findByText(/尚未采集该标的消息/)).toBeTruthy();
  fireEvent.click(screen.getByRole("button", {name:"读取已保存消息"}));
  expect(await screen.findByText(/本次查询未返回消息/)).toBeTruthy();
  expect(fetcher).toHaveBeenCalledTimes(2);
  for (const [url] of fetcher.mock.calls) expect(url).toBe(`/api/v1/instruments/${id}/news`);
});
it("renders untrusted news as text with explicit timing and coverage", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({data:{
    instrument_id:id,status:"saved",symbol:"EXAMPLE",has_more:true,
    received_at:"2026-09-06T00:00:00Z",window_start:"2026-09-05T00:00:00Z",window_end:"2026-09-05T23:45:00Z",
    articles:[{id:1,headline:"<script>attack</script>",summary:"<img src=x onerror=attack>",source:"synthetic",author:"Example",url:null,
      created_at:"2026-09-05T22:00:00Z",updated_at:"2026-09-05T23:00:00Z"}],
  }}))));
  const {container} = render(<SavedNews instrumentId={id}/>);
  expect(await screen.findByText("<script>attack</script>")).toBeTruthy();
  expect(container.querySelector("script, img")).toBeNull();
  expect(screen.getByText(/查询截止至少滞后采集 15 分钟/)).toBeTruthy();
  expect(screen.getByText(/仍有更早消息未读取/)).toBeTruthy();
});
it("never shows a late response for the previous instrument", async () => {
  let resolve!: (value: Response) => void;
  const fetcher = vi.fn().mockImplementationOnce(() => new Promise<Response>((done) => { resolve=done; }))
    .mockResolvedValueOnce(new Response(JSON.stringify({data:{instrument_id:"other",status:"unavailable"}})));
  vi.stubGlobal("fetch",fetcher);
  const {rerender} = render(<SavedNews instrumentId={id}/>);
  rerender(<SavedNews instrumentId="other"/>);
  await screen.findByText(/尚未采集该标的消息/);
  resolve(new Response(JSON.stringify({data:{instrument_id:id,status:"invalid"}})));
  await waitFor(() => expect(screen.queryByText(/原件校验未通过/)).toBeNull());
});
